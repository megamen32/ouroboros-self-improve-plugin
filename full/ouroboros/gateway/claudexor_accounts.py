"""Agent accounts HTTP surface (D30): five THIN proxies, zero auth logic.

Ouroboros's own Claudexor daemon (``claudexor_daemon.py``) owns every account
fact — profiles, login jobs, device-code custody, the two honest verification
statuses, quota windows. The browser cannot talk to the daemon directly (its
control plane is loopback-Origin-guarded and bearer-token'd; the token must
never reach a page), so these handlers translate: status aggregation, the
owner-initiated daemon wake behind the panel's Refresh button, login job
create, login job read/cancel/input, and credential-profile removal. Nothing
here interprets a credential and nothing here stores one.

Login shapes ("красота-сначала", D30): a structural link/device-code card
wherever the engine can host the flow itself — codex device-code today, and
claude/cursor via the engine's disclosure-driven login modes (3.3.7): the job
snapshot's transient overlay carries the ``oauth_url`` sign-in link, and a
claude job additionally accepts the browser's paste-code through
``POST /v2/setup/jobs/{id}/input``. Capability is discovered from the engine's
own ``/v2/operations`` catalog, never assumed. The FALLBACK — only for an
engine that predates those modes — is a copy-paste ``claudexor setup attach
<jobId>`` command the user runs in the USER'S OWN terminal outside this UI,
demoted card-side to a collapsed Advanced affordance. There is no in-app
terminal surface, and none may be added.
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List

from starlette.requests import Request
from starlette.responses import JSONResponse

from ouroboros.gateway._helpers import json_error, request_json_or

log = logging.getLogger(__name__)

# The three read states of one status facet (see the `reads` block below).
READ_OK = "ok"
READ_NOT_READ = "not_read"
READ_FAILED = "failed"

# An EXPLICIT "daemon" transport is DELIBERATELY not accepted from the
# browser: on a pre-disclosure engine it is the macOS Terminal.app handoff,
# and D30 forbids that mechanism outright. Whether an OMITTED transport may
# default to the engine-hosted flow is decided per-engine from the
# /v2/operations catalog (see _login_disclosure_native), never assumed.
_LOGIN_TRANSPORTS = ("", "client_pty")

# The engine advertises its disclosure-driven login modes (3.3.7 contract) by
# implementing the setup-job input route: `POST /v2/setup/jobs/:id/input`
# ships in the same engine change that makes claude/cursor login jobs
# disclosure-driven (oauth_url in the snapshot overlay, no Terminal). One
# structural marker for the one feature, and it is the catalog ID — the
# engine's own stable identifier for the operation, not the path spelling.
_LOGIN_INPUT_OPERATION_ID = "post:setup.jobs.id.input"


def _login_disclosure_native(operations: List[Dict[str, Any]]) -> bool:
    """Does this engine host claude/cursor logins itself (no Terminal)?

    True iff the ``/v2/operations`` catalog advertises the setup-job input
    route under its EXACT catalog id. The path spelling is deliberately NOT a
    second, independent way to answer yes: a route template is a much weaker
    identifier (any engine that ever mounted that shape, under any id, would
    pass), and a false positive here is the expensive direction — it sends an
    OLD engine down the transportless path, whose daemon-side default is the
    macOS Terminal.app handoff D30 forbids. A false NEGATIVE only costs the
    attach fallback, which works on every engine. Pure for unit tests; a
    caller that could not READ the catalog must pass [] and get False."""
    for op in operations:
        if not isinstance(op, dict):
            continue
        if str(op.get("id") or "") == _LOGIN_INPUT_OPERATION_ID:
            return True
    return False


def _login_capable_harness_ids(rows: List[Dict[str, Any]]) -> "set | None":
    """Harnesses whose manifest declares a ``native_session`` auth source.

    That is the engine's own discriminator for "an account you LOG INTO"
    (codex, claude, cursor). The raw-api/openrouter adapters authenticate with
    an API key only — projecting them into the accounts panel gave them
    "Log in" buttons no flow can honor, and leaked them into the reviewer
    slots' subscriptions group. Read from ``/v2/harnesses`` because the
    agent-capability catalog is a derived projection that drops the
    manifest's auth block.

    Returns ``None`` when the response carried ZERO readable manifests overall
    — the answer succeeded but says nothing about auth, so callers must fail
    open exactly like an unreachable read (a blip must not blank the panel).
    Pure for unit tests."""
    capable = set()
    readable = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        manifest = row.get("manifest")
        if not isinstance(manifest, dict):
            continue
        readable += 1
        profile = manifest.get("capability_profile")
        auth = profile.get("auth") if isinstance(profile, dict) else None
        sources = auth.get("supported_sources") if isinstance(auth, dict) else None
        if any(str(s) == "native_session" for s in sources or []):
            capable.add(str(row.get("id") or ""))
    return capable if readable else None


def _account_row_visible(harness_id: str, row: Dict[str, Any], capable) -> bool:
    """Account-surface row predicate: manifest-declared login capability OR the
    daemon vouching an actual login on the row itself (``native_login_detected``)
    — a harness with a null/unreadable manifest must not lose the account the
    owner is really logged into. Deliberately NOT "keep all unknown-manifest
    rows": that would resurrect raw-api (manifest-null on the live engine),
    which is the exact leak this filter closes."""
    if capable is None:
        return True
    return harness_id in capable or bool(row.get("native_login_detected"))


def _status_payload(include_models: bool) -> Dict[str, Any]:
    from ouroboros.claudexor_daemon import get_owned_daemon, owned_config_dir
    from ouroboros.gateways.claudexor import (
        ClaudexorGateway,
        ClaudexorUnavailable,
        discover_daemon_at,
    )
    from ouroboros.subagents import subagent_last_delegation

    daemon = get_owned_daemon().status_dict()
    payload: Dict[str, Any] = {
        "daemon": daemon,
        "config_dir": str(owned_config_dir()),
        "harnesses": [],
        "profiles": {},
        "quota": [],
        # PROVENANCE, per independent facet (BIBLE P1: missing data is a GAP,
        # never a value). `[]`/`{}` alone cannot say WHETHER the daemon was
        # asked: the owner's panel printed "no account connected" for three
        # harnesses while two claude profiles, a cursor profile and two native
        # sessions sat on disk, simply because a lazily-started daemon had not
        # been asked. `ok` — this facet was read and its collection is
        # AUTHORITATIVE (empty means empty); `not_read` — never asked, either
        # because no daemon was running or because discovery/handshake died
        # BEFORE the fan-out and left every facet untouched; `failed` — asked, and
        # no usable answer came back: the read refused, or the body arrived in a
        # shape the facet does not promise. Facets are
        # independent because one fan-out read can fail while its siblings land.
        "reads": {
            "catalog": READ_NOT_READ,
            "accounts": READ_NOT_READ,
            "quota": READ_NOT_READ,
        },
        # The Subagents section's «last delegated run» receipt — Ouroboros's
        # own projection, not daemon truth, so it is served even with the
        # daemon down. {} = no delegated run recorded (absence, not a default).
        "subagent_last_delegation": subagent_last_delegation(),
    }
    if daemon.get("state") != "running":
        return payload
    try:
        endpoint = discover_daemon_at(owned_config_dir())
        with ClaudexorGateway(endpoint) as gateway:
            gateway.handshake()
            payload["daemon"]["engine_version"] = gateway.engine_version
            # The catalog, manifest, profile and quota reads are INDEPENDENT GETs
            # over one thread-safe httpx client, and each costs SECONDS daemon-side
            # (it probes the real coding-agent CLIs on every read: binary, version,
            # login state). Serialized, the panel waited for their SUM — ~23s on a
            # warm daemon with nothing on screen; fanned out it waits for the
            # slowest. Failure semantics are per-facet: each result is classified
            # on its own (see `_facet_outcome`), a refusal surfaces as the typed
            # unreachable state below WITHOUT downgrading the siblings that
            # landed, and a manifest refusal still fails OPEN.
            with ThreadPoolExecutor(max_workers=4) as pool:
                catalog_call = pool.submit(gateway.agent_capabilities)
                manifests_call = pool.submit(gateway.harnesses)
                profiles_call = pool.submit(gateway.credential_profiles)
                quota_call = pool.submit(gateway.quota_snapshots)
            # Classify every submitted future INDEPENDENTLY, before consuming
            # any of them. Reading `.result()` in sequence would make a facet's
            # verdict depend on which sibling raised first — a catalog failure
            # would leave `accounts` reported as unread even though its own read
            # succeeded. Each facet answers only for itself.
            catalog_outcome = _facet_outcome(catalog_call, envelope=("harnesses",))
            profiles_outcome = _facet_outcome(
                profiles_call, envelope=("profiles", "harnessAccounts"))
            quota_outcome = _facet_outcome(quota_call)
            payload["reads"] = {
                "catalog": catalog_outcome[0],
                "accounts": profiles_outcome[0],
                "quota": quota_outcome[0],
            }
            first_error = next(
                (outcome[2] for outcome in (catalog_outcome, profiles_outcome, quota_outcome)
                 if outcome[2] is not None),
                None,
            )
            catalog = catalog_outcome[1] if catalog_outcome[0] == READ_OK else {}
            # Account surfaces show only harnesses with a login concept. On a
            # transient manifest-read failure — or a successful read with zero
            # readable manifests (the helper answers None) — fail OPEN
            # (no filter): a blip must not blank the panel. The manifest read is
            # deliberately NOT a facet: it is a filter input, and its failure is
            # already absorbed rather than reported.
            try:
                capable = _login_capable_harness_ids(manifests_call.result())
            except ClaudexorUnavailable:
                capable = None
            rows: List[Dict[str, Any]] = []
            for row in catalog.get("harnesses") or []:
                if not isinstance(row, dict):
                    continue
                if not _account_row_visible(str(row.get("id") or ""), row, capable):
                    continue
                projected = {
                    "id": str(row.get("id") or ""),
                    "display_name": str(row.get("displayName") or row.get("id") or ""),
                    "status": str(row.get("status") or ""),
                    "enabled": bool(row.get("enabled")),
                    "provider_family": str(row.get("providerFamily") or ""),
                    "access_profiles_supported": [
                        str(v) for v in row.get("accessProfilesSupported") or []
                    ],
                }
                if include_models and projected["id"]:
                    try:
                        projected["models"] = gateway.harness_models(projected["id"])
                    except ClaudexorUnavailable as exc:
                        projected["models"] = []
                        projected["models_error"] = exc.code
                rows.append(projected)
            payload["harnesses"] = rows
            profiles = profiles_outcome[1] if profiles_outcome[0] == READ_OK else {}
            accounts = profiles.get("harnessAccounts") if isinstance(profiles, dict) else None
            if capable is not None and isinstance(accounts, list):
                # The daemon emits a native pseudo-row for EVERY adapter,
                # including the API-key-only ones; same filter, same reason —
                # but a vouched login (native_login_detected) keeps its row
                # even when the manifest is unreadable.
                profiles["harnessAccounts"] = [
                    r for r in accounts
                    if isinstance(r, dict)
                    and _account_row_visible(str(r.get("harness_id") or ""), r, capable)
                ]
            wrappers = profiles.get("profiles") if isinstance(profiles, dict) else None
            if capable is not None and isinstance(wrappers, list):
                # Named credential-profile wrappers leak the same way: an
                # api_key-kind profile registered for a non-loginable harness
                # would render a fake-loginable account row. Keep a wrapper
                # only for a login-capable harness OR one the daemon vouches a
                # native login for among the pseudo-rows above.
                vouched = {
                    str(r.get("harness_id") or "")
                    for r in (accounts if isinstance(accounts, list) else [])
                    if isinstance(r, dict) and r.get("native_login_detected")
                }
                profiles["profiles"] = [
                    w for w in wrappers
                    if isinstance(w, dict)
                    and isinstance(w.get("profile"), dict)
                    and str(w["profile"].get("harness_id") or "") in (capable | vouched)
                ]
            payload["profiles"] = profiles
            payload["quota"] = quota_outcome[1] if quota_outcome[0] == READ_OK else []
            if first_error is not None:
                # At least one facet refused while others landed. The daemon is
                # disclosed as unreachable AND the surviving facets keep their
                # own `ok` — the panel shows what was genuinely read instead of
                # blanking, and never presents an unread facet as empty.
                raise first_error
    except ClaudexorUnavailable as exc:
        payload["daemon"]["state"] = "unreachable"
        payload["daemon"]["last_error"] = f"{exc.code}: {exc}"
        # A failure BEFORE the fan-out (discovery, handshake) leaves every facet
        # at its `not_read` default; those never asked stay not_read, and the
        # ones that were asked and refused are already marked failed above.
    return payload


def _facet_outcome(call: "Future", *, envelope: tuple = ()) -> tuple:
    """Classify ONE fanned-out read: (state, value, error).

    Independent by construction — a sibling's exception can never downgrade a
    facet whose own read landed, and the verdict does not depend on completion
    or consumption order (the pool has already joined when this runs).

    ``envelope`` names EVERY key the facet's own reader promises to deliver;
    all of them must be present for the body to count as the shape we asked
    for. Requiring only one was its own version of the bug: the accounts
    envelope carries named profiles AND native rows, and half an envelope made
    the missing half an authoritative empty. This is not schema validation; it
    closes ONE reachable case, and TWO different bodies reach it. A NON-OBJECT
    body — null, a list, a string — is collapsed by the transport into an empty
    ``{}`` (both ``ClaudexorGateway.agent_capabilities`` and
    ``credential_profiles`` end in ``return body if isinstance(body, dict) else
    {}``), so it arrives here already looking like a legitimate empty answer. A
    body that IS an object but has drifted its keys is NOT touched by the
    transport and arrives intact. Either would otherwise be published as an
    AUTHORITATIVE empty — exactly the lie the read block exists to stop — and
    both land on the same verdict here. An envelope carrying none of its keys is
    a read that did not answer, not an account store that is empty. Quota passes
    ``()``: its reader already filters to a list of rows, and an empty quota
    renders as a neutral absence rather than a verdict.
    """
    from ouroboros.gateways.claudexor import ClaudexorUnavailable

    try:
        value = call.result()
    except ClaudexorUnavailable as exc:
        return (READ_FAILED, None, exc)
    if not isinstance(value, (dict, list)):
        return (READ_FAILED, None, None)
    if envelope and not (isinstance(value, dict) and all(key in value for key in envelope)):
        return (READ_FAILED, None, None)
    return (READ_OK, value, None)


async def api_claudexor_status(request: Request) -> JSONResponse:
    """GET /api/claudexor/status[?include=models] — owned-daemon state plus the
    daemon's own account/quota/catalog truth. Read-only; never spawns."""
    include_models = "models" in str(request.query_params.get("include") or "")
    try:
        return JSONResponse(await asyncio.to_thread(_status_payload, include_models))
    except Exception as exc:
        log.exception("api_claudexor_status failed")
        return json_error(f"{type(exc).__name__}: Claudexor status failed")


async def api_claudexor_wake(request: Request) -> JSONResponse:
    """POST /api/claudexor/wake — OWNER-initiated: start the owned daemon, then
    answer with the freshly read status.

    The status GET stays side-effect-free by contract (and by test), which is
    right for a 5s poll but leaves the panel's Refresh POWERLESS: the daemon is
    lazy, so an owner who just wants to SEE their accounts had to start a login
    job or a delegated run to wake it. This is that missing owner action, and
    nothing else calls it — no poll, no page load.

    Provisioning cost rides here honestly: a cold runtime install happens inside
    this request rather than behind a silent GET.
    """
    from ouroboros.claudexor_daemon import ensure_owned_gateway
    from ouroboros.gateways.claudexor import ClaudexorUnavailable

    def _wake() -> Dict[str, Any]:
        gateway = ensure_owned_gateway()
        gateway.close()
        return _status_payload(include_models=False)

    try:
        return JSONResponse(await asyncio.to_thread(_wake))
    except ClaudexorUnavailable as exc:
        # Typed refusal (missing binary, foreign home, a daemon that never
        # published a descriptor): the panel keeps its rows and says why.
        return json_error(f"{exc.code}: {exc}", 503)
    except Exception as exc:
        log.exception("api_claudexor_wake failed")
        return json_error(f"{type(exc).__name__}: Claudexor wake failed")


def _build_login_request(harness: str, profile_id: str, transport: str,
                         login_flow: str, *, disclosure_native: bool = False) -> Dict[str, Any]:
    """The setup-job request body, honoring the engine's wire contract.

    Pure so the harness-specific transport rule is unit-testable without a
    daemon. The rule mirrors Claudexor's own setup-transport refinement, not
    Ouroboros policy: codex client_pty (the terminal-attach fallback) REQUIRES
    loginFlow=browser_redirect — device/app-server flows are daemon-owned and a
    client_pty job without it is a hard 400 — and loginFlow exists ONLY for
    codex, so it is never sent for another harness (that too is a 400).

    ``disclosure_native`` is the /v2/operations answer (see
    _login_disclosure_native): on such an engine a NON-codex login omits the
    transport so the engine hosts the flow itself and discloses the sign-in
    link through the snapshot overlay — no Terminal, no attach command needed.
    On an older engine the omitted transport would default daemon-side to the
    macOS Terminal.app handoff D30 forbids, so client_pty is forced instead:
    the card polls the sealed job and offers the copy-paste attach command as
    the demoted Advanced fallback.
    """
    request: Dict[str, Any] = {"harness": harness, "action": "login", "authRequest": "subscription"}
    if profile_id:
        request["profileId"] = profile_id
    if harness != "codex" and not transport and not disclosure_native:
        transport = "client_pty"
    if transport:
        request["transport"] = transport
    if harness == "codex" and transport == "client_pty":
        login_flow = login_flow or "browser_redirect"
    if login_flow and harness == "codex":
        request["loginFlow"] = login_flow
    return request


def _login_create(body: Dict[str, Any]) -> Dict[str, Any]:
    from ouroboros.claudexor_daemon import attach_login_command, ensure_owned_gateway

    harness = str(body.get("harness") or "").strip()
    if not harness:
        raise ValueError("harness is required")
    profile_id = str(body.get("profile_id") or "").strip()
    transport = str(body.get("transport") or "").strip()
    if transport not in _LOGIN_TRANSPORTS:
        raise ValueError("transport must be omitted or 'client_pty' (the Terminal.app handoff transport is not offered)")
    login_flow = str(body.get("login_flow") or "").strip()
    # Provisioning moment: the FIRST login action is what spawns the owned
    # daemon (and thereby flips default discovery to it) — an owner action,
    # never a boot-time side effect.
    with ensure_owned_gateway() as gateway:
        # Capability, not folklore: the engine's own route catalog says whether
        # it hosts claude/cursor logins itself (disclosure-driven, 3.3.7). A
        # catalog read failure fails CLOSED onto the attach fallback — it works
        # on every engine, while a mis-defaulted transport on an old engine
        # would be the forbidden Terminal.app handoff.
        try:
            disclosure_native = _login_disclosure_native(gateway.operations())
        except Exception:
            log.debug("operations catalog read failed; assuming pre-disclosure engine", exc_info=True)
            disclosure_native = False
        if profile_id:
            try:
                gateway.create_credential_profile(harness, profile_id)
            except Exception:
                # An existing registration is fine; the login job below is
                # what decides whether the profile is usable.
                log.debug("credential-profile create skipped", exc_info=True)
        request_body = _build_login_request(
            harness, profile_id, transport, login_flow,
            disclosure_native=disclosure_native)
        job = gateway.setup_job_create(request_body)
    job_id = str(job.get("id") or job.get("jobId") or "")
    out = {"job": job, "job_id": job_id, "disclosure_native": disclosure_native}
    if request_body.get("transport") == "client_pty" and job_id:
        # The fallback card's copy-paste command, run OUTSIDE this UI. Read
        # from the REQUEST actually sent (the non-codex default is forced
        # inside the builder, not in the caller's local variable).
        out["attach_command"] = attach_login_command(job_id)
    return out


async def api_claudexor_login(request: Request) -> JSONResponse:
    """POST /api/claudexor/login — create one login job on the owned daemon."""
    from ouroboros.gateways.claudexor import ClaudexorUnavailable

    body: Dict[str, Any] = await request_json_or(request, {})
    try:
        return JSONResponse(await asyncio.to_thread(_login_create, dict(body or {})))
    except ValueError as exc:
        return json_error(str(exc), 400)
    except ClaudexorUnavailable as exc:
        return json_error(f"{exc.code}: {exc}", 503)
    except Exception as exc:
        log.exception("api_claudexor_login failed")
        return json_error(f"{type(exc).__name__}: Claudexor login failed")


def _login_job_call(job_id: str, op: str, value: str = "") -> Dict[str, Any]:
    from ouroboros.claudexor_daemon import attach_login_command, owned_config_dir
    from ouroboros.gateways.claudexor import ClaudexorGateway, discover_daemon_at

    endpoint = discover_daemon_at(owned_config_dir())
    with ClaudexorGateway(endpoint) as gateway:
        gateway.handshake()
        answer = gateway.setup_job_call(job_id, op, value=value)
    if op == "snapshot":
        return {"job": answer, "attach_command": attach_login_command(job_id)}
    if op == "input":
        return {"ok": True, "job": answer}
    return answer


async def api_claudexor_login_job(request: Request) -> JSONResponse:
    """The job-scoped login proxy — one endpoint, three verbs:

    - ``GET /api/claudexor/login/{job_id}`` — poll: the snapshot carries the
      transient disclosure (device code / oauth_url) when the flow has one.
    - ``DELETE /api/claudexor/login/{job_id}`` — the card's cancel action.
    - ``POST /api/claudexor/login/{job_id}/input`` — forward ONE line of user
      input (the claude OAuth paste-code) to the engine's setup-job input
      route. The value rides through and is never logged or stored here.
      Capability degradation is TYPED: an engine that predates the input
      route (or no longer knows the job) answers 404, which comes back as
      ``code=input_not_supported`` so the card can fall back to the Advanced
      attach affordance instead of showing a raw error.
    """
    from ouroboros.gateways.claudexor import ClaudexorUnavailable

    job_id = str(request.path_params.get("job_id") or "").strip()
    if not job_id:
        return json_error("job_id is required", 400)
    is_input = request.method == "POST"
    value = ""
    if is_input:
        # STRICT, and strict in the proxy sense: a JSON OBJECT carrying a
        # `value` that is ALREADY a string of the length the engine accepts.
        # Nothing is coerced (`str(123)` is not a sign-in code) and nothing is
        # rewritten — an edge that trims decides for the engine what the user
        # typed, and a length check on the trimmed form is not the check the
        # engine performs. A non-object body is refused here rather than
        # raising an AttributeError one line later.
        body: Any = await request_json_or(request, {})
        raw = body.get("value") if isinstance(body, dict) else None
        if not isinstance(raw, str) or not raw:
            return json_error("value is required (a non-empty JSON string)", 400)
        if len(raw) > 1024:
            # The engine's ControlSetupJobInputRequest caps the value at 1024
            # chars; refuse at this edge with the same bar (a paste-code is
            # one short line — an oversized body is a caller bug).
            return json_error("value is implausibly long for a sign-in code", 400)
        value = raw
    op = "input" if is_input else ("cancel" if request.method == "DELETE" else "snapshot")
    try:
        return JSONResponse(await asyncio.to_thread(_login_job_call, job_id, op, value))
    except ClaudexorUnavailable as exc:
        status = int(getattr(exc, "status_code", 0) or 0)
        if is_input and status == 404:
            return json_error(
                "This engine does not accept sign-in codes for this job "
                "(input route not available)", 404, code="input_not_supported")
        if is_input and status == 409:
            # The engine's TYPED input conflicts ride through verbatim —
            # setup_input_not_applicable (the callback already completed; no
            # code needed) and setup_input_already_submitted (a repeat the
            # server refused). Answers, not failures: the card maps the code
            # to friendly copy.
            return json_error(str(exc), 409, code=exc.code)
        return json_error(f"{exc.code}: {exc}", 503)
    except Exception as exc:
        log.exception("api_claudexor_login_job failed (%s)", op)
        return json_error(f"{type(exc).__name__}: Claudexor login job {op} failed")


def _remove_credential_profile(harness: str, profile_id: str) -> Dict[str, Any]:
    from ouroboros.claudexor_daemon import owned_config_dir
    from ouroboros.gateways.claudexor import ClaudexorGateway, discover_daemon_at

    endpoint = discover_daemon_at(owned_config_dir())
    with ClaudexorGateway(endpoint) as gateway:
        gateway.handshake()
        gateway.delete_credential_profile(harness, profile_id)
    return {"ok": True, "harness": harness, "profile_id": profile_id}


async def api_claudexor_credential_profile(request: Request) -> JSONResponse:
    """DELETE /api/claudexor/credential-profiles/{harness}/{profile_id}.

    A FIFTH thin proxy, same rule as the other four: the daemon owns the
    account record, so removing one is its own contract
    (``DELETE /v2/credential-profiles/:harness/:profileId``) and its refusal is
    the answer. Nothing here touches a vendor credential file — a native CLI
    login has no route because this process cannot honestly sign it out.
    """
    from ouroboros.gateways.claudexor import ClaudexorUnavailable

    harness = str(request.path_params.get("harness") or "").strip()
    profile_id = str(request.path_params.get("profile_id") or "").strip()
    if not harness or not profile_id:
        return json_error("harness and profile_id are required", 400)
    try:
        return JSONResponse(
            await asyncio.to_thread(_remove_credential_profile, harness, profile_id))
    except ClaudexorUnavailable as exc:
        return json_error(f"{exc.code}: {exc}", 503)
    except Exception as exc:
        log.exception("api_claudexor_credential_profile failed")
        return json_error(f"{type(exc).__name__}: Claudexor account removal failed")


__all__ = [
    "api_claudexor_credential_profile",
    "api_claudexor_login",
    "api_claudexor_login_job",
    "api_claudexor_status",
    "api_claudexor_wake",
]
