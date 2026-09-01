"""Typer control plane for Oracle Lab."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

import typer

from oracle_lab.events import Actor, ActorKind
from oracle_lab.jsonutil import json_default
from oracle_lab.public_view import public_view
from oracle_lab.sbx_probe import SbxProbeError, observe_and_archive_no_model_sbx
from oracle_lab.services import OracleLabService, ServiceError
from oracle_lab.worker_readiness import inspect_worker_readiness

app = typer.Typer(no_args_is_help=True, help="Event-driven R1 exploration and curation")
session_app = typer.Typer(no_args_is_help=True, help="Create, inspect, and branch sessions")
tool_app = typer.Typer(no_args_is_help=True, help="Route explicit tool requests")
sandbox_app = typer.Typer(no_args_is_help=True, help="Inspect sandbox activity")
jobs_app = typer.Typer(invoke_without_command=True, help="Inspect and retry queued work")
export_app = typer.Typer(no_args_is_help=True, help="Export research artifacts")
provenance_app = typer.Typer(no_args_is_help=True, help="Trace provenance graphs")
research_app = typer.Typer(no_args_is_help=True, help="Query research sequences")
replay_app = typer.Typer(no_args_is_help=True, help="Replay fixed history or host analysis")
worker_app = typer.Typer(no_args_is_help=True, help="Run explicitly enabled Host workers")
worker_enqueue_app = typer.Typer(no_args_is_help=True, help="Enqueue worker tasks")
worker_isolation_app = typer.Typer(
    no_args_is_help=True,
    help="Run explicit no-model coding-worker isolation observations",
)
worker_patch_app = typer.Typer(no_args_is_help=True, help="Inspect and judge candidate patches")

app.add_typer(session_app, name="session")
app.add_typer(tool_app, name="tool")
app.add_typer(sandbox_app, name="sandbox")
app.add_typer(jobs_app, name="jobs")
app.add_typer(export_app, name="export")
app.add_typer(provenance_app, name="provenance")
app.add_typer(research_app, name="research")
app.add_typer(replay_app, name="replay")
app.add_typer(worker_app, name="worker")
worker_app.add_typer(worker_enqueue_app, name="enqueue")
worker_app.add_typer(worker_isolation_app, name="isolation")
worker_app.add_typer(worker_patch_app, name="patch")

_service_factory: Callable[[], OracleLabService] = OracleLabService.default


def set_service_factory(factory: Callable[[], OracleLabService]) -> None:
    """Inject a service for tests or an embedding host."""
    global _service_factory
    _service_factory = factory


@app.callback()
def root(context: typer.Context) -> None:
    # Keep control-plane diagnostics side-effect free.  Commands that need the
    # durable service initialize it on first use through ``_service`` instead
    # of creating the database, archives, and worker router for every help or
    # readiness invocation.
    context.obj = {"service": None}


def _service(context: typer.Context) -> OracleLabService:
    root_context = context.find_root()
    service = root_context.obj["service"]
    if service is None:
        service = _service_factory()
        root_context.obj["service"] = service
    return service


def _emit(value: Any) -> None:
    typer.echo(
        json.dumps(
            public_view(value),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=lambda item: public_view(json_default(item)),
        )
    )


def _call(function: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    try:
        _emit(function(*args, **kwargs))
    except (ServiceError, KeyError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


@session_app.command("new")
def session_new(
    context: typer.Context,
    title: Annotated[str | None, typer.Option("--title", help="Human-readable title")] = None,
    model: Annotated[str | None, typer.Option("--model", help="Model profile ID")] = None,
) -> None:
    _call(_service(context).new_session, title, model_profile_id=model)


@session_app.command("import")
def session_import(
    context: typer.Context,
    source: Annotated[
        str,
        typer.Argument(help="JSON/JSONL conversation log or research-bundle directory"),
    ],
    title: Annotated[str | None, typer.Option("--title", help="Imported session title")] = None,
    authorize_human_curation: Annotated[
        bool,
        typer.Option(
            "--authorize-human-curation",
            help="Explicitly preserve human keep/star/note curation from a bundle",
        ),
    ] = False,
) -> None:
    _call(
        _service(context).import_session,
        source,
        title=title,
        authorize_human_curation=authorize_human_curation,
        authorizer=(Actor(kind=ActorKind.HUMAN, id="cli") if authorize_human_curation else None),
    )


@session_app.command("list")
def session_list(context: typer.Context) -> None:
    _call(_service(context).list_sessions)


@session_app.command("show")
def session_show(context: typer.Context, session_id: str) -> None:
    _call(_service(context).show_session, session_id)


@session_app.command("switch")
def session_switch(context: typer.Context, session_id: str) -> None:
    _call(_service(context).switch_session, session_id)


@session_app.command("checkpoint")
def session_checkpoint(
    context: typer.Context,
    note: Annotated[str | None, typer.Option("--note")] = None,
) -> None:
    _call(_service(context).checkpoint, note)


@session_app.command("fork")
def session_fork(
    context: typer.Context,
    event_id: str,
    name: Annotated[str | None, typer.Option("--name", help="Branch title")] = None,
) -> None:
    _call(_service(context).fork, event_id, name)


@session_app.command("approve-fork")
def session_approve_fork(context: typer.Context, proposal_event_id: str) -> None:
    _call(_service(context).approve_branch, proposal_event_id)


@session_app.command("archive")
def session_archive(
    context: typer.Context,
    session_id: Annotated[str | None, typer.Argument()] = None,
) -> None:
    _call(_service(context).archive_session, session_id)


@app.command("ask")
def ask(
    context: typer.Context,
    text: str,
    model: Annotated[str | None, typer.Option("--model")] = None,
) -> None:
    _call(_service(context).ask, text, model_profile_id=model)


@app.command("continue")
def continue_command(
    context: typer.Context,
    model: Annotated[str | None, typer.Option("--model")] = None,
) -> None:
    _call(_service(context).continue_session, model_profile_id=model)


@app.command("sample")
def sample(
    context: typer.Context,
    count: Annotated[int, typer.Option("--count", "-n", min=1)] = 1,
    temperature: Annotated[float | None, typer.Option("--temperature")] = None,
    top_p: Annotated[float | None, typer.Option("--top-p")] = None,
    model: Annotated[str | None, typer.Option("--model")] = None,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
    from_event_id: Annotated[str | None, typer.Option("--from")] = None,
) -> None:
    _call(
        _service(context).sample,
        count,
        temperature=temperature,
        top_p=top_p,
        model_profile_id=model,
        session_id=session_id,
        from_event_id=from_event_id,
    )


@app.command("retry")
def retry(context: typer.Context, event_id: str) -> None:
    _call(_service(context).retry, event_id)


@app.command("pause")
def pause(
    context: typer.Context,
    note: Annotated[str | None, typer.Option("--note")] = None,
) -> None:
    _call(_service(context).pause, note)


@app.command("resume")
def resume(
    context: typer.Context,
    note: Annotated[str | None, typer.Option("--note")] = None,
) -> None:
    _call(_service(context).resume, note)


@app.command("events")
def events(context: typer.Context) -> None:
    _call(_service(context).list_events)


@app.command("tail")
def tail(
    context: typer.Context,
    limit: Annotated[int, typer.Option("--limit", min=1)] = 20,
) -> None:
    _call(_service(context).tail, limit)


@app.command("show")
def show(context: typer.Context, event_id: str) -> None:
    _call(_service(context).show_event, event_id)


@app.command("tree")
def tree(context: typer.Context) -> None:
    _call(_service(context).event_tree)


@app.command("trace")
def trace(context: typer.Context, event_id: str) -> None:
    _call(_service(context).trace_event, event_id)


@provenance_app.command("trace")
def provenance_trace(context: typer.Context, derived_kind: str, derived_id: str) -> None:
    _call(_service(context).provenance_trace, derived_kind, derived_id)


@provenance_app.command("event")
def provenance_event(context: typer.Context, event_id: str) -> None:
    _call(_service(context).trace_event, event_id)


@app.command("keep")
def keep(context: typer.Context, event_id: str) -> None:
    _call(_service(context).keep, event_id)


@app.command("canonize")
def canonize(context: typer.Context, candidate_event_id: str) -> None:
    """Explicitly approve one analysis.canon_candidate as Human."""
    _call(_service(context).approve_canon_candidate, candidate_event_id)


@app.command("reject")
def reject(context: typer.Context, event_id: str) -> None:
    _call(_service(context).reject, event_id)


@app.command("star")
def star(context: typer.Context, event_id: str) -> None:
    _call(_service(context).star, event_id)


@app.command("quarantine")
def quarantine(
    context: typer.Context,
    event_id: str,
    note: Annotated[str | None, typer.Option("--note")] = None,
) -> None:
    _call(_service(context).quarantine, event_id, note)


@app.command("revisit")
def revisit(
    context: typer.Context,
    event_id: str,
    note: Annotated[str | None, typer.Option("--note")] = None,
) -> None:
    _call(_service(context).revisit, event_id, note)


@app.command("note")
def note(context: typer.Context, event_id: str, text: str) -> None:
    _call(_service(context).note, event_id, text)


@app.command("pin-claim")
def pin_claim(context: typer.Context, claim_id: str) -> None:
    _call(_service(context).pin_claim, claim_id)


@app.command("claims")
def claims(context: typer.Context) -> None:
    _call(_service(context).claims)


@app.command("contradictions")
def contradictions(context: typer.Context) -> None:
    _call(_service(context).contradictions)


@app.command("motifs")
def motifs(context: typer.Context) -> None:
    _call(_service(context).motifs)


@app.command("attractors")
def attractors(context: typer.Context) -> None:
    _call(_service(context).attractors)


@research_app.command("contradiction-mechanisms")
def research_contradiction_mechanisms(
    context: typer.Context,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
) -> None:
    _call(_service(context).contradiction_mechanism_branches, session_id)


@research_app.command("latex-prefixes")
def research_latex_prefixes(
    context: typer.Context,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
    word_count: Annotated[int, typer.Option("--words", "-n", min=1)] = 5,
) -> None:
    _call(
        _service(context).words_before_latex_attractors,
        session_id=session_id,
        word_count=word_count,
    )


@research_app.command("fork-before-attractor")
def research_fork_before_attractor(
    context: typer.Context,
    attractor_event_id: str,
    title: Annotated[str | None, typer.Option("--name")] = None,
) -> None:
    _call(_service(context).fork_before_attractor, attractor_event_id, title)


@research_app.command("prompt-attractors")
def research_prompt_attractors(
    context: typer.Context,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
    phrase: Annotated[str | None, typer.Option("--phrase")] = None,
) -> None:
    _call(
        _service(context).prompt_attractor_statistics,
        session_id=session_id,
        phrase=phrase,
    )


@app.command("search")
def search(
    context: typer.Context,
    query: str,
    semantic: Annotated[bool, typer.Option("--semantic")] = False,
    limit: Annotated[int, typer.Option("--limit", min=1)] = 20,
) -> None:
    _call(_service(context).search, query, semantic=semantic, limit=limit)


@app.command("origin")
def origin(context: typer.Context, query: str) -> None:
    _call(_service(context).origin, query)


@tool_app.command("run")
def tool_run(context: typer.Context, event_id: str) -> None:
    _call(_service(context).request_tool, event_id)


@tool_app.command("approve")
def tool_approve(context: typer.Context, request_id: str) -> None:
    _call(_service(context).approve_tool, request_id)


@sandbox_app.command("inspect")
def sandbox_inspect(context: typer.Context, identifier: str) -> None:
    _call(_service(context).inspect_sandbox, identifier)


@worker_enqueue_app.command("repository-edit")
def worker_enqueue_repository_edit(
    context: typer.Context,
    source_event_id: Annotated[str, typer.Option("--source")],
    goal: Annotated[str, typer.Option("--goal")],
    repository: Annotated[str, typer.Option("--repository")] = ".",
) -> None:
    _call(
        _service(context).enqueue_repository_edit,
        source_event_id,
        goal,
        repository=repository,
    )


@worker_app.command("status")
def worker_status(context: typer.Context, task_event_id: str) -> None:
    _call(_service(context).worker_task_status, task_event_id)


@worker_app.command("readiness")
def worker_readiness(
    agents_config: Annotated[
        str | None,
        typer.Option(
            "--agents-config",
            help="Path to agents.toml (defaults to ORACLE_LAB_CONFIG/agents.toml)",
        ),
    ] = None,
) -> None:
    """Report static coding-worker prerequisites without starting a worker."""

    path = (
        Path(agents_config)
        if agents_config is not None
        else Path(os.environ.get("ORACLE_LAB_CONFIG", "config")) / "agents.toml"
    )
    report = inspect_worker_readiness(path)
    _emit(report.to_dict())
    if not report.ready:
        raise typer.Exit(code=1)


@worker_isolation_app.command("probe")
def worker_isolation_probe(
    archive_root: Annotated[
        str,
        typer.Option(
            "--archive-root",
            help="Operator-owned directory outside the target repository",
        ),
    ],
    sbx_executable: Annotated[
        str,
        typer.Option("--sbx-executable", help="Standalone Docker sbx executable"),
    ] = "sbx",
    sandbox_name: Annotated[
        str | None,
        typer.Option(
            "--sandbox-name",
            help="Optionally inspect one already-existing sandbox by its exact name",
        ),
    ] = None,
    observe_read_only_control_plane: Annotated[
        bool,
        typer.Option(
            "--observe-read-only-control-plane",
            help="Explicitly allow read-only sbx version/list/inspect commands",
        ),
    ] = False,
) -> None:
    """Archive a read-only real sbx observation without issuing attestation."""

    if not observe_read_only_control_plane:
        _emit(
            {
                "schema_version": 1,
                "status": "blocked",
                "reason_id": "read_only_sbx_observation_not_confirmed",
                "ready": False,
                "safe_to_start_worker": False,
                "attestation_issued": False,
            }
        )
        raise typer.Exit(code=1)
    try:
        report, archive = observe_and_archive_no_model_sbx(
            archive_root=archive_root,
            sandbox_name=sandbox_name,
            executable=sbx_executable,
        )
    except SbxProbeError as error:
        _emit(
            {
                "schema_version": 1,
                "status": "failed",
                "reason_id": error.reason_id,
                "ready": False,
                "safe_to_start_worker": False,
                "attestation_issued": False,
            }
        )
        raise typer.Exit(code=1) from None
    _emit({**report.to_public_dict(), "archive": archive.to_public_dict()})
    if report.status != "observed":
        raise typer.Exit(code=1)


@worker_patch_app.command("show")
def worker_patch_show(context: typer.Context, patch_event_id: str) -> None:
    _call(_service(context).patch_show, patch_event_id)


@worker_patch_app.command("approve")
def worker_patch_approve(context: typer.Context, patch_event_id: str) -> None:
    _call(_service(context).approve_patch, patch_event_id)


@worker_patch_app.command("reject")
def worker_patch_reject(
    context: typer.Context,
    patch_event_id: str,
    reason: Annotated[str | None, typer.Option("--reason")] = None,
) -> None:
    _call(_service(context).reject_patch, patch_event_id, reason=reason)


@worker_patch_app.command("status")
def worker_patch_status(context: typer.Context, patch_event_id: str) -> None:
    _call(_service(context).patch_status, patch_event_id)


@app.command("run")
def run(
    context: typer.Context,
    until_human: Annotated[bool, typer.Option("--until-human")] = False,
    max_jobs: Annotated[int, typer.Option("--max-jobs", min=1)] = 100,
) -> None:
    _call(
        _service(context).run_automation,
        until_human=until_human,
        max_jobs=max_jobs,
    )


@jobs_app.callback()
def jobs(context: typer.Context) -> None:
    if context.invoked_subcommand is None:
        _call(_service(context).list_jobs)


@jobs_app.command("retry")
def jobs_retry(
    context: typer.Context,
    job_id: Annotated[str | None, typer.Argument()] = None,
) -> None:
    _call(_service(context).retry_jobs, job_id)


@app.command("cost")
def cost(
    context: typer.Context,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
    model_id: Annotated[str | None, typer.Option("--model")] = None,
) -> None:
    _call(_service(context).cost, session_id=session_id, model_id=model_id)


@app.command("compare-models")
def compare_models(
    context: typer.Context,
    model_profile_ids: Annotated[list[str], typer.Argument(help="Model profile IDs")],
    session_id: Annotated[str, typer.Option("--session")],
    event_id: Annotated[str, typer.Option("--event")],
) -> None:
    _call(
        _service(context).compare_models,
        session_id=session_id,
        event_id=event_id,
        model_profile_ids=model_profile_ids,
    )


@replay_app.command("exact")
def replay_exact(
    context: typer.Context,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
    branch_id: Annotated[str | None, typer.Option("--branch")] = None,
    record: Annotated[
        bool,
        typer.Option("--record/--no-record", help="Append a session.replayed audit event"),
    ] = True,
) -> None:
    _call(
        _service(context).replay_exact,
        session_id=session_id,
        branch_id=branch_id,
        record=record,
    )


@replay_app.command("host")
def replay_host(
    context: typer.Context,
    event_id: Annotated[str, typer.Argument(help="Historical oracle.output event ID")],
    host_profile_label: Annotated[
        str | None,
        typer.Option(
            "--host-profile",
            help="Explicit host analysis profile label; required when a router is configured",
        ),
    ] = None,
) -> None:
    _call(
        _service(context).replay_host_analysis,
        event_id,
        host_profile_label=host_profile_label,
    )


@export_app.command("bundle")
def export_bundle(
    context: typer.Context,
    destination: str,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
) -> None:
    _call(_service(context).export, "bundle", destination, session_id=session_id)


@export_app.command("public-bundle")
def export_public_bundle(
    context: typer.Context,
    destination: str,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
) -> None:
    _call(_service(context).export, "public-bundle", destination, session_id=session_id)


@export_app.command("transcript")
def export_transcript_command(
    context: typer.Context,
    destination: str,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
) -> None:
    _call(_service(context).export, "transcript", destination, session_id=session_id)


@export_app.command("corpus")
def export_corpus(
    context: typer.Context,
    destination: str,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
) -> None:
    _call(_service(context).export, "corpus", destination, session_id=session_id)


@app.command("tui")
def tui(
    context: typer.Context,
    session_id: Annotated[str | None, typer.Option("--session")] = None,
) -> None:
    from oracle_lab.tui import run_tui

    run_tui(_service(context), session_id=session_id)


if __name__ == "__main__":  # pragma: no cover
    app()


__all__ = ["app", "set_service_factory"]
