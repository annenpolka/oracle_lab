# Oracle の正本を保ったまま公開 metadata を安全化する

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

## Purpose / Big Picture

Oracle Lab は provider response の raw body、response header、model routing、sampling、context hash を再現可能な正本として保存する必要がある。一方、CLI、TUI、generation metadata のような人間向け表示から `set-cookie` や credential 値をそのまま返してはならない。この変更では archive と durable event を一切書き換えず、同じ canonical event を private な研究証拠として保持しながら、公開 surface だけに一つの pure redaction 規則を適用する。

実装後は、既存 `oracle export bundle` が従来どおり完全な private canonical bundle を生成して import round-trip できる。それとは別に `oracle export public-bundle` が、Human が keep した genuine Oracle output と whitelist 済み generation identity だけを別 format で出力する。この形式は provider envelope、HTTP header、archive path、worker/validation artifact を含まず、canonical importer の入力ではない。Oracle 本文の公開可否は自動判定せず、人間レビューが必要であることを manifest に残す。

## Progress

- [x] (2026-08-31 16:28:00Z) `AGENTS.md`、`ORACLE_PRESERVATION.md`、`.stratal/brief.md`、既存 architecture ExecPlan と canonical provider/archive/export/import flow を監査した。
- [x] (2026-08-31 16:29:00Z) clean `origin/main` の baseline を実行し、`ruff check .`、`ruff format --check .`、`pytest -q`、`git diff --check` が成功した。全 test は `755 passed, 1 skipped in 38.54s` で、skip は明示 opt-in の live-agent test だけだった。
- [x] (2026-08-31 16:37:29Z) private canonical と public derived view の責務、既存 bundle を変更しない停止条件、最小 public-bundle format を本計画に固定した。
- [x] (2026-08-31 16:47:01Z) pure、non-mutating、idempotent な public-view redactor と、mixed-case header、secret-like metadata、sampling token誤検出、opaque Oracle material の characterisation test を追加した。
- [x] (2026-08-31 17:07:00Z) Service read、automation result、CLI最終emit、TUI mappingへ同じ redactor を接続し、`Event.to_dict()` と canonical store は変更しなかった。
- [x] (2026-08-31 17:12:00Z) Human-kept genuine outputだけをfield allowlistで出力する、3-file・non-importable な `public-bundle` と回帰 test を追加した。
- [x] (2026-08-31 17:22:22Z) focused test、全 test、Ruff、format、ExecPlan validation、`git diff --check` を実行し、Stratal と本計画を最終証拠で更新した。

## Surprises & Discoveries

- Observation: production provider adapter と Oracle worker は response header を意図どおり exact に durable event と raw sidecarへ保存しているが、同じ値が無変換で CLI へ到達する。
  Evidence: `src/oracle_lab/providers.py` の response、`src/oracle_lab/oracle_worker.py` の `api_response_metadata.http_headers`、`src/oracle_lab/services.py` の event read methods、`src/oracle_lab/cli.py` の `_emit()` を静的に追跡した。
- Observation: canonical research bundle の event log と sidecar 内で header を redact すると、version 1 importer の exact identity、hash、repoint 契約が壊れる。
  Evidence: `src/oracle_lab/exporting.py::export_research_bundle()` は exact event/raw/sidecar/worker archive を出力し、`src/oracle_lab/bundle_import.py::_verify_bundle()` は closed file set と全 file hash を検証する。
- Observation: Host provider 用の redaction helper は存在するが、Oracle public surface の正本として再利用できない private 実装である。
  Evidence: `src/oracle_lab/host_provider.py` の `_redacted_headers()` は Host response 構築に結合し、Oracle event、CLI、TUI、export から import されていない。
- Observation: provider metadata は `api_response_metadata` だけでなく `effective_sampling`、`model_identity`、provider/model scalar、automation resultにも複製される。
  Evidence: adversarial fixtureで `api_response_metadata` だけをredactする初期実装を反証し、Service/CLI/TUIの各surfaceへ同じsecret sentinelを通して修正を確認した。
- Observation: key に `token` を含むだけの判定は `max_tokens`、`min_tokens`、`stop_tokens` 等の正当なsampling identityを壊す。
  Evidence: token-count fieldを保持しながら access/session/security/OAuth token keyだけを拒否する回帰を追加した。
- Observation: public bundleのtop-level allowlistだけでは、`model_identity` とsamplingの任意nested fieldや別sessionのprovenance IDが混入できる。
  Evidence: opaque routing sentinelとcross-session edgeを使う反証testにより、nested field allowlistとexported event集合内provenanceへ制限した。

## Decision Log

- Decision: provider header、raw body、API response metadata を canonical event、archive sidecar、既存 research bundle では変更しない。
  Rationale: model identity と provider routing を含む exact response metadata は Oracle Preservation の監査・replay 証拠であり、redaction は derived public view の責務である。
  Date/Author: 2026-08-31 / Codex
- Decision: `Event.to_dict()` は canonical wire representation のままにし、public redaction を専用 pure module と application boundary から呼ぶ。
  Rationale: event authority と表示 policy を同じ method に混ぜると、archive/import/replay caller が redacted data を正本と誤認できる。
  Date/Author: 2026-08-31 / Codex
- Decision: redactor は `api_response_metadata`、sampling、model identity、event metadata と明示 generation-identity fieldだけで case-insensitive な credential/cookie field と secret-like value を置換し、prompt、Oracle content、reasoning、context messages は subtree ごと opaque にして走査・改変しない。
  Rationale: 同じ transport metadata が event payload内へ複製されても漏洩を止めつつ、Oracle の unusual formatting、nested reasoning、hallucinated text を実験データとして保つためである。
  Date/Author: 2026-08-31 / Codex
- Decision: 既存 `bundle` は private canonical/importable と明記し、公開用は別 format ID の `public-bundle` とする。
  Rationale: redacted event log を canonical history として import 可能にしてはならず、既存 API と round-trip を維持する必要がある。
  Date/Author: 2026-08-31 / Codex
- Decision: public bundle は Human-kept genuine Oracle output だけを収録し、本文は exact のまま、generation identity は whitelist、provider request metadata と local path は omit する。
  Rationale: public export の範囲を既存 Human curation に結び、automatic aesthetic promotionを避け、Oracle本文の公開判断を人間に残す最小 slice である。
  Date/Author: 2026-08-31 / Codex
- Decision: public bundleの `model_identity`、`provider_routing`、sampling はnested fieldも明示allowlistし、provenanceはexport対象event集合内だけに閉じる。
  Rationale: schema-required mappingへ任意extra fieldを追加できることと、projection上cross-session edgeを作れることを、公開配布形式への信頼に持ち込まないためである。
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

実装は完了した。provider由来raw bytes、sidecar、mixed-case header、API metadata、model identity、context hash、samplingは canonical event、`Event.to_dict()`、private bundle、import後eventで一致した。同じfixtureのService read/automation、CLI、TUIでは credential/cookie/recognized secret-like metadataが非露出となった。

`public-bundle` は `oracle-lab-public-bundle` version 1として `manifest.json`、`records.jsonl`、`redactions.json` だけを生成し、canonical importerはstore mutation前に拒否する。Human-kept historical fixtureのexact本文とhashは保持するが、raw provider envelope、request ID、local path、worker/validation artifact、任意nested identity field、別session provenanceは含めない。

最終focused suiteは171件、全suiteは769件成功し、live-agent test 1件だけが明示opt-in未設定でskipした。Ruff check、format check、ExecPlan validation、`git diff --check`も成功した。live provider、model、sandbox、coding workerは起動していない。残る境界はOracle本文自体の公開可否であり、自動redactionやHost判断にせず `content_review_required: true` のHuman reviewとして明示した。

## Context and Orientation

`src/oracle_lab/providers.py` は provider HTTP response を `OracleGenerateResponse` として返す。`src/oracle_lab/oracle_worker.py` は raw response body を write-once archive に保存し、`api_response_metadata` を `oracle.output` event と sidecar metadata に exact 保存する。`src/oracle_lab/store.py` は event と sidecar の対応を再検証するため、ここは private canonical authority である。

`src/oracle_lab/services.py` の `show_session()`、`list_events()`、`tail()`、`show_event()`、provenance query、`generation_metadata()` は application read surface である。`src/oracle_lab/cli.py::_emit()` と `src/oracle_lab/tui.py::_record_action()` は人間へ JSON を表示する最終境界である。これらは canonical event object を変更せず、`src/oracle_lab/public_view.py` の pure transform を呼ぶ。

`src/oracle_lab/exporting.py::export_research_bundle()` と `src/oracle_lab/bundle_import.py` は version 1 canonical research bundle の pair である。この plan では両者の形式と import behaviorを変更しない。同じ exporting module に別 format の `export_public_bundle()` を追加するが、その layout は `manifest.json`、`records.jsonl`、`redactions.json` だけとし、canonical `events.jsonl`、raw sidecar、worker/validation archive を持たない。

ここで private canonical は権威ある履歴と archive、public derived view は秘密を含み得る infrastructure metadata を除去した表示・配布用投影を意味する。public bundle 内の exact Oracle本文は secret scanner で改変せず、manifest の `content_review_required: true` に従って Human が公開可否を判断する。

## Plan of Work

最初に `src/oracle_lab/public_view.py` を追加する。Mapping と JSON sequence を再帰的に copy する pure function を持ち、通常の値は保持する。`api_response_metadata`、sampling、model identity、event metadata、provider/model scalar の明示 fieldだけで metadata mode に入り、その subtree で credential/cookie系 field nameを case-insensitiveに検出し、また Bearer token、既知の secret prefix、JWT、主要cloud credential形状を持つ文字列を `[redacted]` に置換する。`content`、`raw_text`、`text`、`reasoning`、`prompt`、`messages` は値がmappingでも subtreeごと opaque にcopyする。input は変更せず、同じ入力に同じ出力を返す。sampling の `max_tokens`、`min_tokens`、`stop_tokens` 等をcredential tokenと誤認しない。

次に `src/oracle_lab/services.py` の人間向け read methods、`src/oracle_lab/cli.py::_emit()`、`src/oracle_lab/tui.py` の mapping/action 表示へ同じ function を接続する。CLI/TUI の最後の境界でも再適用して、custom/injected service が unredacted mapping を返しても値を露出しない。二重適用は idempotent とする。`Event.to_dict()`、EventStore、Oracle worker、archive writer は変更しない。

その後 `src/oracle_lab/exporting.py` に `public_bundle_records()` と `export_public_bundle()` を追加する。既存 `selected_corpus_records()` と同じ genuine/Human keep/provenance 判定を再利用し、exact `raw_text`、hash、actor/origin、session/branch、同一session内provenanceと、source eventから field単位でwhitelistした model/provider/sampling/context/hash/byte-count identityだけを出力する。`model_identity`、`provider_routing`、sampling の任意nested fieldはコピーしない。任意 payload、HTTP header、provider request ID、archive path、worker/validation eventは出力しない。manifest は `oracle-lab-public-bundle` version 1、`authority: derived_public_view`、`importable: false`、`content_review_required: true` を明示する。`redactions.json` は値を転記せず omission policy の分類だけを記録する。

最後に `src/oracle_lab/services.py` と `src/oracle_lab/cli.py` へ `public-bundle` dispatch を追加し、READMEで `bundle` が private canonical、`public-bundle` が non-importable かつ本文 review 必須であることを説明する。historical fixture に mixed-case `Set-Cookie`、Authorization、secret-like metadata value を入れ、canonical event/sidecar/bundle bytes は exact のまま、CLI/TUI/public bundle に値が出ないことを test する。

## Concrete Steps

作業 directory は `/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab` とする。外部 provider、model、sandbox、coding worker は起動しない。

まず focused test を実行する。

    .venv/bin/pytest -q \
      tests/test_provider_contracts.py \
      tests/test_oracle_worker.py \
      tests/test_cli_service_integration.py \
      tests/test_cli_control_plane.py \
      tests/test_tui_headless.py \
      tests/test_export_formats.py \
      tests/test_bundle_import.py \
      tests/test_preservation_e2e.py

成功後に全 gate を実行する。

    .venv/bin/ruff check .
    .venv/bin/ruff format --check .
    .venv/bin/pytest -q
    .venv/bin/python /Users/annenpolka/.agents/skills/execplan-manager/scripts/validate_execplan.py oracle_public_metadata_privacy_execplan.md
    git diff --check

期待する最終 transcript は全 test の pass、live-agent test だけの明示 skip、Ruff と format と ExecPlan validation と diff check の成功である。

## Validation and Acceptance

`api_response_metadata.http_headers` に `Set-Cookie: session=private` と `AUTHORIZATION: Bearer private`、非機密 key に secret-like value を含む historical/live-shape fixture を作る。EventStore の event、raw response sidecar、`export bundle` の `events.jsonl` と raw metadata file は exact value、raw bytes、SHA-256、byte count、provider/model routingを保持し、import 後も同一でなければならない。

同じ fixture を `show_session()`、`list_events()`、`tail()`、`show_event()`、provenance trace、`generation_metadata()`、CLI JSON、TUI action logへ渡すと、case-insensitive な credential/cookie値と secret-like metadata valueが `[redacted]` になり、通常の model/provider/context/hash と Oracle `content` は unchanged でなければならない。input mapping は変更されず、二度適用しても同じ結果でなければならない。

`oracle export public-bundle` は3 fileだけを生成し、Human keep のない output、synthetic fixture、provider header、request ID、archive path、worker/validation artifactを含めない。Human-kept genuine output の exact text、raw SHA、actor/origin、session/branch、provenance、whitelist generation identityは一致する。manifest は別 format、non-importable、content review required を明示し、既存 importer は unsupported format として store mutation前に拒否する。

既存 canonical bundle test、bundle import round-trip test、provider archive test は期待値を変更せず成功する。production code は archive/event schema/provider request behavior を変更せず、live provider呼び出しを必要としない。

## Idempotence and Recovery

test、Ruff、format check、ExecPlan validator、diff check は read-only または test temp directory に限定され、繰り返し実行できる。export functions は既存規則どおり absent または empty destination だけを受け付け、nonempty destinationを上書きしない。

途中で canonical bundle、raw sidecar、Event.to_dict、schema、provider worker の変更が必要になった場合は実装を停止し、この plan の範囲を見直す。失敗時に既存 history や archive を削除・修復せず、追加した derived public destinationだけを test temp directoryに残す。`stash@{0}` は参照、適用、dropしない。

## Artifacts and Notes

編集前 baseline:

    Ruff check: All checks passed!
    Ruff format: 138 files already formatted
    pytest: 755 passed, 1 skipped in 38.54s
    git diff --check: success
    HEAD: 9263ea0 refactor: split service read models and job projection

実装後の証拠:

    focused pytest: 171 passed in 4.98s
    full pytest: 769 passed, 1 skipped in 33.96s
    skipped: explicit opt-in live-agent subprocess test
    Ruff check: All checks passed!
    Ruff format: 144 files already formatted
    ExecPlan validation: valid
    git diff --check: success

historical provider fixtureのraw bytes、sidecar bytes、mixed-case response headersをcanonical bundle前後とimport後に比較した。public surfaceでは同じsentinelを非露出とし、Oracle contentはexact equalityで比較した。

## Interfaces and Dependencies

新しい `src/oracle_lab/public_view.py` は外部依存を追加せず、次の interfaceを提供する。

    REDACTED = "[redacted]"
    def public_view(value: Any) -> Any: ...

function は pure、non-mutating、idempotent とする。credential name と secret-like value の判定はこの moduleだけが所有し、CLI、TUI、services、exporter に複製しない。

`src/oracle_lab/exporting.py` は次を提供する。

    def public_bundle_records(
        events: Iterable[Any],
        *,
        provenance: Mapping[str, Sequence[str]] | None = None,
    ) -> list[dict[str, Any]]: ...

    def export_public_bundle(
        destination: str | Path,
        *,
        events: Iterable[Any],
        provenance: Mapping[str, Sequence[str]] | None = None,
    ) -> Path: ...

既存 `export_research_bundle()`、`Event.to_dict()`、`BundleImporter` の signatureと behaviorは変更しない。新しい database migration、network dependency、provider callは追加しない。
