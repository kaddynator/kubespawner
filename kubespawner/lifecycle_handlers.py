"""
Pluggable pod lifecycle handlers for KubeSpawner.

Provides a strategy pattern so different pod scheduling systems (default
Kubernetes, Kueue, etc.) can customize pod creation, readiness-waiting, and
progress events without adding conditional branches to the core spawner.

Usage via config::

    # Enable Kueue integration
    c.KubeSpawner.kueue_enabled = True
    c.KubeSpawner.kueue_queue_name = "user-queue"
"""

import asyncio
import time

from kubernetes_asyncio import client as k8s_client


class PodLifecycleHandler:
    """Default handler for standard Kubernetes pod lifecycle.

    Submits pods directly to the cluster's scheduler and polls for the
    ``Ready`` condition.  Can be subclassed to change any step without
    modifying the spawner itself.
    """

    async def create_pod(self, v1, namespace, body):
        """Create a pod on the cluster.

        Args:
            v1: kubernetes_asyncio CoreV1Api instance.
            namespace: namespace to create the pod in.
            body: pod manifest dict.
        """
        await v1.create_namespaced_pod(namespace=namespace, body=body)

    async def wait_for_ready(self, v1, pod_name, namespace, timeout, log):
        """Poll until the pod reaches the ``Ready`` condition.

        Args:
            v1: kubernetes_asyncio CoreV1Api instance.
            pod_name: name of the pod to wait for.
            namespace: namespace containing the pod.
            timeout: seconds before giving up and raising TimeoutError.
            log: traitlets logger.

        Raises:
            TimeoutError: if the pod does not become Ready before *timeout*.
        """
        deadline = time.monotonic() + timeout
        log.info("Waiting up to %ss for pod %s to be Ready", timeout, pod_name)

        while time.monotonic() < deadline:
            try:
                pod = await v1.read_namespaced_pod(
                    name=pod_name, namespace=namespace
                )
            except k8s_client.exceptions.ApiException as e:
                if e.status == 404:
                    log.debug("Pod %s not found yet, retrying…", pod_name)
                    await asyncio.sleep(2)
                    continue
                raise

            if pod.status and pod.status.conditions:
                for cond in pod.status.conditions:
                    if cond.type == "Ready" and cond.status == "True":
                        log.info("Pod %s is Ready", pod_name)
                        return

            await asyncio.sleep(2)

        log.warning("Timed out waiting for pod %s to be Ready", pod_name)
        raise TimeoutError(
            f"Pod {pod_name} did not become Ready within {timeout}s"
        )

    async def get_extra_events(self, v1, pod_name, namespace, log):
        """Return additional events to surface in the spawn progress UI.

        The base handler returns an empty list.  Override in subclasses to
        fetch events from related objects (e.g. Kueue Workloads).

        Returns:
            list of event dicts (kubernetes ``Event`` objects as dicts).
        """
        return []


class KueuePodLifecycleHandler(PodLifecycleHandler):
    """Handler for pods managed by `Kueue <https://kueue.sigs.k8s.io/>`_.

    Kueue adds a ``SchedulingGate`` to each managed pod, holding it in
    ``SchedulingGated`` state until the workload is admitted to a
    ``ClusterQueue``.  This handler:

    - Logs ``SchedulingGated`` → ``Scheduled`` transitions so operators can
      see queue admission in Hub logs.
    - Surfaces Kueue ``Workload`` events (quota checks, flavor selection,
      admission decisions) in the JupyterHub spawn progress UI so users know
      why their server is queued.
    - Falls back gracefully to standard pod-Ready polling once the gate is
      lifted.

    Configure via ``jupyterhub_config.py``::

        c.KubeSpawner.kueue_enabled = True
        c.KubeSpawner.kueue_queue_name = "user-queue"
    """

    def __init__(self, queue_name=None):
        """
        Args:
            queue_name: name of the Kueue ``LocalQueue`` this pod is submitted
                to.  Used only in user-facing progress messages; does not
                affect routing.
        """
        self.queue_name = queue_name

    async def wait_for_ready(self, v1, pod_name, namespace, timeout, log):
        """Wait for a Kueue-managed pod, logging scheduling-gate transitions.

        Args:
            v1: kubernetes_asyncio CoreV1Api instance.
            pod_name: name of the pod to wait for.
            namespace: namespace containing the pod.
            timeout: seconds before giving up and raising TimeoutError.
            log: traitlets logger.

        Raises:
            TimeoutError: if the pod does not become Ready before *timeout*.
        """
        deadline = time.monotonic() + timeout
        was_gated = False
        q_suffix = f" (queue: {self.queue_name})" if self.queue_name else ""
        log.info(
            "Waiting up to %ss for Kueue-managed pod %s to be Ready%s",
            timeout,
            pod_name,
            q_suffix,
        )

        while time.monotonic() < deadline:
            try:
                pod = await v1.read_namespaced_pod(
                    name=pod_name, namespace=namespace
                )
            except k8s_client.exceptions.ApiException as e:
                if e.status == 404:
                    log.debug("Pod %s not found yet, retrying…", pod_name)
                    await asyncio.sleep(2)
                    continue
                raise

            if pod.status and pod.status.conditions:
                is_gated = False
                for cond in pod.status.conditions:
                    if cond.type == "Ready" and cond.status == "True":
                        log.info("Pod %s is Ready", pod_name)
                        return
                    if (
                        cond.type == "PodScheduled"
                        and cond.reason == "SchedulingGated"
                    ):
                        is_gated = True
                        if not was_gated:
                            log.info(
                                "Pod %s is waiting for Kueue admission%s",
                                pod_name,
                                q_suffix,
                            )

                if was_gated and not is_gated:
                    log.info(
                        "Kueue admitted pod %s — scheduling now%s",
                        pod_name,
                        q_suffix,
                    )
                was_gated = is_gated

            await asyncio.sleep(2)

        log.warning(
            "Timed out waiting for Kueue-managed pod %s to be Ready", pod_name
        )
        raise TimeoutError(
            f"Pod {pod_name} did not become Ready within {timeout}s (Kueue)"
        )

    async def get_extra_events(self, v1, pod_name, namespace, log):
        """Fetch Kueue ``Workload`` events for the spawn progress UI.

        Kueue creates a ``Workload`` custom resource named
        ``pod-{pod_name}-{hash}`` for each gated pod and posts events about
        quota checks, flavor selection, and admission decisions on it.  This
        method fetches those events so users see queue progress in the
        JupyterHub spawn UI, not just a blank waiting screen.

        Returns:
            list of Kubernetes Event dicts, empty list on any error.
        """
        workload_prefix = f"pod-{pod_name}-"
        try:
            result = await v1.list_namespaced_event(
                namespace=namespace,
                field_selector="involvedObject.kind=Workload",
            )
            matched = [
                e.to_dict()
                for e in (result.items or [])
                if e.involved_object
                and (e.involved_object.name or "").startswith(workload_prefix)
            ]
            if matched:
                log.debug(
                    "Found %d Kueue workload event(s) for pod %s",
                    len(matched),
                    pod_name,
                )
            return matched
        except Exception:
            log.debug(
                "Could not fetch Kueue workload events for pod %s", pod_name
            )
            return []
