# 公開契約を保ったまま Oracle Lab の内部構造を整理する

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

## Purpose / Big Picture

この変更の目的は、Oracle Lab が現在守っている実験記録、raw bytes、provenance（由来を示す参照関係）、truth domain（観測結果が real、sandbox、virtual、retrieved、synthetic のどれに属するか）、Human gate、coding-worker の fail-closed 境界を一切弱めず、変更箇所を探しやすくすることである。利用者から見た CLI、event schema、archive layout、model identity、prompt 原文、例外型は変えない。

実装後は、queue、projection、worker の参照用問い合わせ、研究用問い合わせが責務別の小さな module に置かれる。`oracle_lab.services.OracleLabService` は既存の公開入口として残るが、SQLite の読み取り詳細や独立した変換規則を抱え込まない。成果は既存 CLI と全 test が同じ結果を返すこと、runtime import cycle が解消すること、`services.py` の直接 SQL と行数が減ることで観察する。

この計画は機能追加でも archive hardening でもない。現在知られている `web_verify="auto"` の branch 分離迂回、Oracle response header の public redaction、raw archive 親 directory の symlink hardening、DNS rebind 対策は重要だが、外部挙動を変えるため別の修正として扱う。未完成の `stash@{0}` にある privileged-isolation 実験は参照も適用もせず、現行 HEAD と混ぜない。

## Progress

- [x] (2026-08-31 14:12:00Z) 規範文書、Stratal、既存 coding-worker ExecPlan、manifest、lockfile、全 production/test surface を読み、調査範囲を固定した。
- [x] (2026-08-31 14:31:00Z) Phase 1 の census を完了し、46 production modules、37,592 production LOC、66 test modules、21,485 test LOC を記録した。
- [x] (2026-08-31 14:48:00Z) Oracle、tool/virtual、coding-worker/patch の主要 flow を入口から archive、event、継続まで追跡した。
- [x] (2026-08-31 14:57:00Z) dependency、security、typing、error、state、test seam を横断監査し、反証調査で実装範囲を leaf/read-side-first に絞った。
- [x] (2026-08-31 15:04:00Z) 編集前 baseline として `ruff check .`、`ruff format --check .`、全 `pytest`、`git diff --check` を実行した。結果は `719 passed, 1 skipped in 39.80s`、`125 files already formatted`、その他は成功だった。
- [x] (2026-08-31 15:25:00Z) Milestone 1: job contract と job projection を queue 実装から分離し、旧 symbol identity と pickle lookup を維持したまま runtime cycle を1から0へ解消した。focused testは23件成功した。
- [x] (2026-08-31 15:26:00Z) Milestone 2: worker projection の read model を抽出し、既存 `OracleLabService` method、`ServiceError`、返却shapeを互換 façade として残した。focused testは59件、共有worktreeの全suiteは723件成功した。
- [x] (2026-08-31 15:40:00Z) Milestone 3a: claims、contradictions、motifs、attractors の研究catalog queryを `ResearchCatalogReadModel` へ抽出した。全session scope、synthetic lineage除外、列と順序、read-only性を9件のfocused testで固定した。
- [x] (2026-08-31 15:58:00Z) Milestone 3b: prompt-attractor統計、LaTeX prefix、検索の三queryを同じread modelへ抽出した。façadeでのactive session解決とvalidation、`origin()`から`self.search()`への互換seamを残し、合同focused testは42件成功した。
- [x] (2026-08-31 16:01:00Z) Milestone 3b後の全gateを実行し、`730 passed, 1 skipped in 40.16s`、Ruff check/format、`git diff --check`が成功した。skipは明示opt-inのlive-agent testだけだった。
- [x] (2026-08-31 16:05:00Z) Milestone 3: research queryとusage/cost queryを責務別read modelへ抽出した。usage focused testは81件成功し、service内のdirect SQLは17から11へ減った。provenance exportとsession/controlに結合したqueryは停止条件に従い残した。
- [x] (2026-08-31 16:05:00Z) Milestone 4: tool-resultの機械整形とloop signature、worker/validation artifactのmanifest viewだけをpure helperへ抽出した。known/unknown archive decodeはfailure contractが異なるため残した。統合後focused testは135件成功した。
- [x] (2026-08-31 16:07:00Z) Milestone 5:全 regression gate、import graph、LOC、direct SQL、公開 identity を再計測した。最終結果は`755 passed, 1 skipped in 41.13s`、Ruff check/format、`git diff --check`成功、runtime SCC 0だった。
- [x] (2026-08-31 16:08:00Z) Stratal、coding-worker ExecPlan、本計画のProgress、Decision、Discovery、Outcomeを最終証拠と残存境界で更新した。

## Surprises & Discoveries

- Observation: `src/oracle_lab/services.py` は 7,208 行、`OracleLabService` は 7,005 行、148 methods であるが、Git の変更頻度が高い file ではない。
  Evidence: reachable HEAD の主要実装はすべて初回 commit `a0997e9` で一括追加され、`services.py` はその後変更されていない。履歴は分割境界の根拠にならず、静的依存と test 契約を根拠にする必要がある。
- Observation: eager import graph に cycle はないが、function-local import を含めると `jobs -> store -> projections -> jobs` の runtime cycle が一つある。
  Evidence: `src/oracle_lab/jobs.py` は `store` を eager import し、`store.py` は projection 適用時に `projections` を import し、`projections.py` の default registry は `jobs.JobProjection` を import する。
- Observation: `OracleLabService` 全体や worker execution を最初に composition object へ移すと、16 constructor states と archive、queue、Git、sandbox、recovery の依存を複製する。
  Evidence: `_branch_service` と `_append` は各24回、`_active` は22回、`_job_queue` は15回 self-call される。private service calls は少なくとも16 test files、118箇所にある。
- Observation: 重複して見える archive writer、freeze helper、tool execution、patch application は同じ障害契約を持たない。
  Evidence: raw response archive は失敗時にも authoritative raw orphan を残し、worker/validation archive は partial artifact を cleanup して同一 run を回復する。bundle import はさらに `O_NOFOLLOW` と上位 rollback ledger を持つ。
- Observation: checked-in default は安全側だが、refactor と混ぜるべきでない既存 gap がある。
  Evidence: `web_verify="auto"` は verification fork を迂回でき、Oracle response header は未 redaction のまま durable event/export surface へ流れる。既定 policy は `ask`、verification allowlist は空である。
- Observation: public classをinternal moduleへ置いても、旧moduleから同一objectをre-exportし `__module__` を維持すれば、pickle lookupとPydantic schemaを保ったままruntime import edgeを切れた。
  Evidence: `tests/test_job_contract_compatibility.py` が `JobStatus`、`Job`、`JobProjection` のobject identity、pickle round-trip、schema fingerprintを固定し、job/projection focused test 23件が成功した。
- Observation: 最小read modelはfaçade callbackなしに切り出せた。
  Evidence: `WorkerReadModel` は `EventStore` だけを保持し、read-only testはevent IDsとSQLite `total_changes`がquery前後で同一であることを確認した。service direct SQLは17から13へ減った。
- Observation: 研究catalogの四queryはactive sessionを暗黙選択せず、global scopeのまま独立したread modelへ移せた。
  Evidence: `ResearchCatalogReadModel` は `EventStore` だけを保持し、cross-sessionの行を返すこと、synthetic lineageを除外すること、event IDsとSQLite `total_changes`を変えないことをfocused testで確認した。
- Observation: active sessionの選択はpure readではなく、単一session時にcontrol fileを書き得る。
  Evidence: session-scoped研究queryはfaçadeで従来どおり`_active()`を呼んでから明示的session IDをread modelへ渡した。active/explicit scope、literal `%_[` search、返却順、validation errorをcharacterization testで固定した。
- Observation: direct `connection.execute` の単純な半減は、安全な責務分離の良い代理指標ではなかった。
  Evidence: usage抽出後に残る11箇所はsession fallback/control mutation、queue pause、runtime-config fallback、database path safety、patch/canon/claim Human mutation、pending judgmentと、export専用のraw-row gatewayである。これらを別objectへ移すにはfaçade callbackまたはmutation stateの複製が必要になる。
- Observation: archive周辺で同じ形に見えるknown/unknown decodeは、malformed時に`None`、`False`、または例外を返す異なるrecovery contractを持つ。
  Evidence: Milestone 4では共通化せず、入力だけで決まるtool formatting/signatureと、既にarchive済みのconcrete recordからEventStoreが再検証するmanifest viewだけを抽出した。

## Decision Log

- Decision: `OracleLabService` と `ServiceError` は `oracle_lab.services` に残し、CLI と TUI の公開 façade を維持する。
  Rationale: repository 内だけでも `OracleLabService` import は多数あり、external consumer は不明である。module identity を変える利益より互換性リスクが大きい。
  Date/Author: 2026-08-31 / Codex
- Decision: whole-service または whole-worker extraction ではなく、transaction と filesystem mutation を持たない leaf/read-side から始める。
  Rationale: read model は `EventStore` と明示的 scope だけで成立する一方、worker execution は archive-first recovery と nested transaction に強く結合している。
  Date/Author: 2026-08-31 / Codex
- Decision: archive writer、freeze helper、state literal を lexical similarity だけで共通化しない。
  Rationale: accepted input、failure cleanup、truth domain、persisted state machine が異なる。共通化は同値な pure primitive を test で証明できる場合に限る。
  Date/Author: 2026-08-31 / Codex
- Decision: migration v1 から v4 は immutable な履歴として扱い、本 refactor では編集しない。
  Rationale:四つの migration は同一 bootstrap commit 由来で、Python runner の変更は既存 SQL checksum に反映されない。移動や一般化の価値より drift risk が高い。
  Date/Author: 2026-08-31 / Codex
- Decision: known security gaps は Discovery と follow-up として記録し、behavior-preserving refactor の patch に混ぜない。
  Rationale:修正には policy、export metadata、HTTP transport、filesystem behavior の変更が必要で、原因追跡と rollback を難しくするためである。
  Date/Author: 2026-08-31 / Codex
- Decision: Milestone 5の構造判定は、当初のdirect SQL半減を強制せず、17から11への削減、`self._rows()` call siteの8から5への削減、runtime SCCの解消、façade LOC削減を組み合わせて評価する。
  Rationale: 残存queryはmutation、control-file、archive exportまたはsafety checkと不可分であり、数値を8以下にするための移動はStratalの停止条件に反する。research側の三SQLは元からgeneric `_rows()`経由だったため、direct-call数だけでは実際のownership移動も数えられない。
  Date/Author: 2026-08-31 / Codex
- Decision: Milestone 4はtool pure helperとartifact manifest viewだけを採用し、archive record class、writer、required filename、known/null decodeを統合しない。
  Rationale: 採用したhelperはstore、filesystem、clock、policyを持たずexact outputでcharacterizeできる一方、除外した候補はfailure/recovery semanticsが一致しない。
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

2026-09-01、六つの静的調査 phaseとMilestone 1から5を完了した。job contract/projection分離によりruntime import SCCは1から0になった。worker、research、usage/cost read modelによりservice direct SQLは17から11、`self._rows()` callerは8から5へ減った。toolの機械整形/signatureとartifact manifest viewはpure ownerへ移り、known/unknown recovery decodeは意図的に残した。`services.py`は7,208行から6,839行、`OracleLabService`は7,005行から6,655行、148 methodsから145 methodsへ減った。production全体は明示的componentのmodule境界により46 modules/37,592 LOCから52 modules/37,808 LOCへ216行増えたが、巨大façadeから369行を移し、依存の小さいownerとcharacterization testへ置き換えた。testsは66 modules/21,485 LOCから70 modules/22,614 LOCになった。

最終gateは`755 passed, 1 skipped in 41.13s`、`ruff check .`、138 filesの`ruff format --check .`、`git diff --check`が成功した。skipは`tests/test_live_agent_opt_in.py`の明示opt-in gateだけで、live SBX、sandbox、OracleProvider、model、coding workerは起動していない。旧job import/pickle/Pydantic identity、projection rebuild、ServiceError、global/active scope、synthetic lineage除外、read-only性、cost shape、tool text/digest、archive manifest shapeはcharacterization testで維持された。

残した重複と境界は意図的である。`_rows()`の5 callerはbranch raw-row viewとarchive/export専用queryで、controlまたはfilesystem export境界から分離しなかった。session、patch/canon/claim approval、pending judgmentの11 direct SQLはquery後にcontrol-file、transaction、queue、Human eventまたはsafety checkを持つ。worker/validation/SBXのknown/null decode、archive writer、freeze helperはfailure/recovery contractが異なるため共通化しなかった。`contradiction_mechanism_branches()`と`origin()`はBranchService、trace/provenance、既存façade seamを合成するため残した。既知のverification policy、Oracle header redaction、DNS pinning、raw archive parent symlink、dependency auditは本refactorと分離したfollow-upのままである。

## Context and Orientation

repository root は `/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab` である。Python 3.13 以上を使う単一 package で、console entrypoint は `pyproject.toml` の `oracle = "oracle_lab.cli:app"` である。`src/oracle_lab/cli.py` は必要になるまで service を生成しない。help、worker readiness、read-only SBX observation が database、archive、coding-worker bind を副作用として起動しないため、この lazy construction を維持する。

authoritative data は `src/oracle_lab/store.py` の append-only event log と `src/oracle_lab/archive.py` などの write-once artifact である。projection は event から再構築できる派生 table で、event append と同期 projection は同じ SQLite transaction に入る。queue の mutable row と lifecycle event も同じ transaction で更新される。この二つの atomicity は module 分割後も変えない。

Oracle generation の順序は human input、oracle request、queue、context event、provider call、raw response file、sidecar、oracle output と usage の atomic append、rendering、Host analysis である。raw archive は database transaction の外側で output event より先に書く。database append が失敗すると raw orphan が残り得るが、それが現在の復旧可能な preservation contract である。

tool flow は durable request と Human approval を検証してから broker を呼び、tool result、usage、mechanical adapter、Oracle continuation の順で append する。virtual runtime は explicit operation が必要とする最小 state だけを Host-originated event として materialize する。coding-worker flow は現在の標準 construction では conformance evidence 不足により fail closed である。test から注入する fake worker seam と production authorization を統合してはならない。

read model とは、authoritative event や projection tableを読み、表示用の immutable な辞書を返す object である。read model は event append、job enqueue、archive write、filesystem mutationを行わない。本計画ではこの性質を抽出境界に使う。

主要 hotspot は `src/oracle_lab/services.py` 7,208 行、`store.py` 2,343 行、`agent_adapters.py` 2,058 行、`projections.py` 1,583 行、`virtual.py` 1,405 行、`tooling.py` 1,390 行である。production は46 files、37,592 LOC、tests は66 files、21,485 LOC である。public package root は `new_id` だけを export するが、各 module の `__all__` と direct imports があるため、module-level symbol も互換対象として扱う。

## Plan of Work

Milestone 1 では queue model、queue projection、mutable queue の三責務を分ける。`src/oracle_lab/jobs.py` にある `JobStatus`、`Job`、job lifecycle event set を小さな internal contract moduleへ置き、`JobProjection` を store 非依存の projection moduleへ置く。`oracle_lab.jobs.JobStatus`、`Job`、`JobProjection` は同一 object を re-export し、既存の `__module__`、pickle lookup、Pydantic schema、event payload keyを characterization test で固定する。`src/oracle_lab/projections.py` の default registry は新 projection moduleを直接参照し、`jobs -> store -> projections -> jobs` edge を消す。`EventStore.append_many()` と projection の同一 transaction は変更しない。

Milestone 2 では `worker_task_status()`、`patch_show()`、`patch_status()` の SQL と JSON decode を `src/oracle_lab/worker_read_model.py` の internal objectへ移す。object は `EventStore` だけを受け取り、read operation だけを提供する。`OracleLabService` の同名 public methods は薄い委譲として残し、`ServiceError` の型、message、dict key、status literalを維持する。approve、reject、apply、validation、archive recovery はこの milestone では動かさない。

Milestone 3 では `claims()`、`contradictions()`、`motifs()`、`attractors()`、prompt-attractor 統計、contradiction-mechanism query、LaTeX prefix query、search、origin、usage/model comparison のうち、mutationを行わず少数依存で閉じるものを `src/oracle_lab/research_read_model.py` と必要なら `experiment_read_model.py` へ移す。active session/branch の選択は façade が行い、read model には明示的 ID を渡す。`fork_before_attractor()`、replay、export、Host analysis のような mutation または外部境界を持つ操作は残す。

Milestone 4 では static method または局所関数で、入力から同じ出力を返す純粋処理だけを domain moduleへ移す。候補は tool result の mechanical content、loop signature、worker archive manifest の読み取り view、known/unknown archive observation の decodeである。既存 private method をtestsが呼ぶ場合は wrapperを残す。approval proof、archive write、raw bytes、Git application、sandbox executionをこの milestoneの共通 helperへ含めない。

Milestone 5 では全 test を再実行し、import graph と metrics を比較する。目標は runtime SCC を1から0へ、`services.py` の direct `store.connection.execute` callを17から11以下、generic `self._rows()` call siteを8から5以下へ減らし、`OracleLabService` の行数を7,005から減少させることである。当初のdirect SQL半減目標は、mutation/control/archive境界を別objectへ隠す誘因になると判明したためDecision Logの複合判定へ置き換えた。数値を満たすために圧縮記法や巨大 helperへ押し込むことは禁止する。新 component が façade callback、archive root、router、provider、job queueを不必要に要求した場合、その抽出を戻して小さい境界を採用する。

## Concrete Steps

すべての command は repository root `/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab` で実行する。live SBX、Docker sandbox、OracleProvider、model、coding-agent executable は起動しない。

編集前 baseline は完了している。再現 command と期待値は次のとおりである。

    .venv/bin/ruff check .
    # All checks passed!

    .venv/bin/ruff format --check .
    # 125 files already formatted

    .venv/bin/pytest -q
    # 719 passed, 1 skipped in 39.80s

    git diff --check
    # no output

各 milestone の前に `git status --short` と `git diff --stat` を確認する。focused tests を先に実行し、成功した場合だけ全 suiteへ進む。Milestone 1 は少なくとも次を使う。

    .venv/bin/pytest tests/test_jobs_queue.py tests/test_job_projection.py tests/test_projection_rebuild.py tests/test_schema_migrations.py -q

Milestone 2 は少なくとも次を使う。

    .venv/bin/pytest tests/test_agent_adapter_service_integration.py tests/test_bundle_import.py tests/test_worker_pipeline_recovery.py -q

Milestone 3 は少なくとも次を使う。

    .venv/bin/pytest tests/test_research_queries.py tests/test_provenance_usage_observability.py tests/test_cost_safeguards.py tests/test_cli_service_integration.py -q

Milestone 4 は影響する既存 test fileに加え、tool、worker archive、validation archive、automation boundaryをまとめて実行する。

    .venv/bin/pytest tests/test_tool_broker.py tests/test_automation_boundaries.py tests/test_worker_archive.py tests/test_validation_archive.py -q

各 milestone後と最終時には次を実行する。

    .venv/bin/ruff check .
    .venv/bin/ruff format --check .
    .venv/bin/pytest -q
    git diff --check

import graph は Python AST を用いた read-only scriptで eager、function-local、`TYPE_CHECKING` edgeを再計測する。baseline は eager edge 106、runtime edge 148、eager SCC 0、runtime SCC 1 である。LOC は `wc -l src/oracle_lab/*.py` と AST class range、direct SQL は `rg -n 'store\.connection\.execute' src/oracle_lab/services.py` で比較する。

## Validation and Acceptance

全体 acceptance は `.venv/bin/pytest -q` が baseline と同じ `719 passed, 1 skipped` 以上で成功し、skip が `tests/test_live_agent_opt_in.py` の明示 opt-in gateだけであることとする。test数が新規 characterizationにより増える場合、pass数は増えてよい。`ruff check .`、`ruff format --check .`、`git diff --check` は成功しなければならない。

Oracle、tool、worker の observable event順序、actor、truth domain、parent、causation、correlation、source IDs、archive path、filename、hash、byte countは既存 integration/recovery test と新しい exact snapshot testで一致させる。raw stdout/stderr、Oracle response body、prompt textは byte-for-byte変えない。synthetic fixtureを genuine Oracle materialとして扱わない。

`oracle_lab.services.OracleLabService` と `ServiceError` のmodule identity、constructor signature、`default()`、CLI service factoryは維持する。`oracle_lab.jobs.JobStatus`、`Job`、`JobProjection` は旧 import pathから同じ objectとして取得でき、`__module__` を含む互換 testを通す。既存 event schema、migration checksum、archive layout、config schema、CLI command/JSON shapeを変更しない。

構造 acceptance は、runtime import cycleが0、`services.py` direct SQLがbaseline 17から11以下、generic `_rows()` call siteが8から5以下、read modelが mutation APIを持たず façade callbackを要求しないことである。残存SQLはownerと非抽出理由をOutcomeへ記録する。単に同じ巨大 classを別fileへ移すこと、mixinでimplicit dependencyを隠すこと、圧縮記法でLOCを減らすことはacceptしない。

## Idempotence and Recovery

各 milestoneは独立してtest可能な小さい diffにする。新 moduleを追加して旧 symbolをre-exportした後にcall siteを切り替え、すべてのtestが通ってから旧実装を削る。途中で失敗した場合は未完成moduleと委譲だけをそのmilestoneのdiffとして調べ、過去commitやstashをworktreeへ適用して復旧しない。

archive、database schema、runtime dataを変更するcommandは使わない。testは一時 directoryとin-memory databaseを使う。`ORACLE_LAB_RUN_LIVE_AGENT_TESTS` を設定せず、外部worker、provider、sandboxを起動しない。migration v1-v4は編集しないためrollback migrationは不要である。

既存 worktreeに新しいuser変更が現れた場合はそのfileを上書きしない。重なる場合は作業を止め、diffとscopeを報告する。`stash@{0}` は保全対象であり、drop、pop、apply、showを行わない。

## Artifacts and Notes

編集前の主要 metrics は次のとおりである。

    production modules: 46
    production LOC: 37,592
    test modules: 66
    test LOC: 21,485
    OracleLabService: 7,005 lines / 148 methods
    services.py runtime dependencies: 34 modules
    services.py direct SQL calls: 17
    eager import SCC: 0
    runtime import SCC: 1 (jobs -> store -> projections -> jobs)
    private service calls in tests: at least 118 across 16 files
    broad except Exception: 11 across 7 production modules
    freeze helpers: 9 with non-identical semantics

編集前 quality transcriptは次のとおりである。

    All checks passed!
    125 files already formatted
    719 passed, 1 skipped in 39.80s
    git diff --check: clean

未解決 security follow-up は、verification fork policy、Oracle header redaction、HTTP DNS pinning、raw archive parent symlink hardening、dependency vulnerability auditである。これらは本計画の refactor差分に混ぜない。

## Interfaces and Dependencies

新しい production dependencyは追加しない。Python標準library、Pydantic、既存の `EventStore` と immutable event typesを使う。

job split後も `from oracle_lab.jobs import Job, JobStatus, JobProjection, JobQueue` が動く。internal moduleは queueのための immutable contractと projectionの `apply(connection: sqlite3.Connection, event: Event) -> None` を提供する。projection nameは `jobs`、tablesは `("jobs",)` のままにする。

`WorkerReadModel` はconstructorで `EventStore` だけを受け取り、`worker_task_status(task_event_id: str) -> dict[str, Any]`、`patch_show(patch_event_id: str) -> dict[str, Any]`、`patch_status(patch_event_id: str) -> dict[str, Any]` と同等のread operationを持つ。domain errorはinternal errorとして表し、façadeが同じmessageの `ServiceError` に変換する。

`ResearchCatalogReadModel` は `EventStore` だけを受け取り、内部で既存 `RetrievalIndex` をqueryごとに構築する。active session/branchを内部で推測せず、query methodの引数として受け取る。event append、queue enqueue、branch fork、archive write、filesystem writeを公開しない。

`UsageCostReadModel` は `EventStore` だけを受け取り、既存aggregate shapeを返す `cost_summary()` と、policy判定前のOracle cost rowを返す `oracle_cost_records()` だけを公開する。policy、clock、Decimal計算、warning/error event、queue判断はfaçadeに残す。

pure helperは入力値だけから決定的に出力し、store、clock、environment、filesystem、networkへアクセスしない。既存 private methodを互換wrapperとして残す場合、wrapperは新helperを一度だけ呼ぶ。

tool helperは `tooling.py` の `mechanical_tool_result_content()` と `tool_loop_signature()` が所有する。artifact helperは既にarchive writerが発行したartifact recordを `{name: {path, sha256, size_bytes}}` のevent viewへ変換するだけで、file read、rehash、path解決、record validationを行わない。authoritativeなintegrity検証は従来どおりarchive writerと`EventStore`に残る。

`OracleLabService` は引き続きTyperとTextualが使う同期application boundaryである。constructor injection、lazy config、provider factory、host worker router、job handler、control state、fail-closed default assemblyはこの計画で変更しない。
