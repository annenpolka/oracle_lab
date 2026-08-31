# コーディングエージェント運用連携を完成させる

このExecPlanはリビングドキュメントである。実装中は `Progress`、
`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective` を
継続的に更新し、途中で作業を止める場合も現在地と残作業を記録する。

## Purpose / Big Picture

Oracle LabからCodexまたはOpenCodeへ調査、テスト生成、コード変更を依頼し、
その結果を追跡可能な候補成果物として保存できるようにする。

コーディングエージェントはOracleではなく、非信頼のHostワーカーとして扱う。
エージェントが生成した文章、コード、diff、テスト結果をOracle由来の情報として
保存してはならない。コード変更は直接メイン作業ツリーへ適用せず、隔離された
作業領域でcandidate patchを生成し、Oracle Labがpatch、実行条件、出典、検証結果を
write-once archiveへ保存する。適用には明示的な人間の承認を必要とする。

実装後、利用者はsource eventからworker taskを作り、CodexまたはOpenCodeが生成した
candidate patchを確認し、人間承認後にsource-independent standalone staging cloneへ適用し、sandbox上の
検証結果までイベント履歴から再構築できる。自動commit、push、merge、および現在の
作業ツリーへの暗黙的な変更は行わない。

観察可能な最終フローは次のとおりである。

    source event
      -> worker task
      -> Codex/OpenCode run
      -> candidate patch
      -> deterministic security preflight
      -> human approval
      -> standalone staging cloneへの適用
      -> sandbox validation

## Progress

- [x] (2026-08-30 20:45:18Z) 既存のadapter、router、service接続、統合テストの現状を確認した。
- [x] (2026-08-30 20:59:03Z) Milestone 3〜6を静的監査し、patch回収、専用Human gate、staging適用、repository validationが未実装であることを確認した。
- [x] (2026-08-30 20:59:03Z) 運用フローをsecurity preflight → human approval → persistent staging apply → sandbox validationに統一し、READMEとlive opt-inの安全な骨格を追加した。実agentの起動はまだ有効化していない。
- [x] (2026-08-30 21:43:27Z) Host worker設定を標準CLIから読み込み、checked-in設定をdisabled-by-defaultにした。
- [x] (2026-08-30 21:43:27Z) worker prompt、argv、version、stdout/stderr、patch、identityをdurable write-once archiveへ接続した。
- [x] (2026-08-30 23:03:09Z) repository editの結果をwrite-once candidate patchとして保存し、worker用cloneとは別のtrusted standalone cloneでpatchを回収した。sourceのHEAD、index、filesystem、Git control dataを監査し、target preconditionをHuman gate前に再検証するようにした。
- [x] (2026-08-30 21:43:27Z) candidate patchに対するHuman-only承認・拒否と、承認＋apply enqueueの原子化を実装した。
- [x] (2026-08-30 23:03:09Z) 承認済みpatchを外部のpersistent standalone staging cloneへ適用し、remote、alternates、sourceとの共有objectを持たせず、partial stagingは破棄してbase commitから再作成するようにした。
- [x] (2026-08-30 23:03:09Z) frozen sandbox設定でstaging treeをDocker validation経路へ搬入し、approval、requested/actual image identity、exact ToolResult status/error、raw stdout/stderr、`truth_domain=sandbox`をwrite-once archiveへ保存した。
- [x] (2026-08-30 23:03:09Z) 失敗、設定回数内の再試行、タイムアウト、output limit、重複実行、branch単位pause、lease heartbeat、atomic enqueue、dead-letter retry、verified orphan archiveからの1回限定recovery-only leaseを実装した。
- [x] (2026-08-30 23:03:09Z) 明示注入したfake coding agentによるtask → archive → candidate patch → Human gate → standalone staging clone → sandbox validationの決定論的E2Eを実装した。
- [ ] 特権OS isolation brokerとconformance testを実装し、その後にfixture repositoryだけを対象とする実Codex/OpenCode CLI smoke testを行う。（型契約、bounded export、synthetic protocol lifecycleは実装済みだが、production evidenceと実agent smokeは未完了）
- [x] (2026-08-30 22:08:07Z) 特権隔離brokerが存在しない現状を運用可能と誤認させないため、標準config/default serviceで有効化されたCodex/OpenCodeをrouter構築時、subprocess起動前にfail closedするgateを追加した。fake adapterは明示的なdependency injectionに限定した。
- [x] (2026-08-30 21:43:27Z) worker enqueue/status/patch show/approve/reject/status CLIを実装した。
- [x] (2026-08-30 21:43:27Z) worker由来の直接・推移的lineageをclaim/motif/curation/corpusから除外し、bundle importをhistorical-only authorityとして隔離した。
- [x] (2026-08-30 21:43:27Z) worker home/archive/workspace/stagingをtarget repositoryとcurrent worktreeの外に限定し、default cwd実行がsource内へ`.oracle_lab`を作らない回帰試験を追加した。
- [x] (2026-08-30 23:03:09Z) forged Oracle origin、lease owner省略、concurrent enqueue、validation tree TOCTOU、contradictory validation terminalをStore/queue/service境界で拒否する回帰を追加した。
- [x] (2026-08-30 23:13:28Z) ExecPlan validator、Ruff format/check、compileall、全体pytest、既報境界の最終回帰を同一スナップショットで通過させた。実agent外部呼び出しは行っていない。
- [x] (2026-08-31 02:40:15Z) Docker Sandbox microVMを候補backendとして選定し、旧`docker sandbox` pluginを有効化せず、現行`sbx`だけを対象とするconformance-gated実装方針を固定した。実Codex/OpenCode呼び出しはoperator opt-inまで行わない。
- [x] (2026-08-31 03:22:03Z) coding-worker isolationの必須capability、profile-bound attestation、capabilityごとのpassing check、receipt hash/timestamp、cleanup-confirmed resultを型契約と決定論的テストで固定した。
- [x] (2026-08-31 03:22:03Z) VM workspaceを自己申告counterや無制限copyで受け取らず、圧縮・hardlink・device・sparse構文を持たないbounded custom archiveとして搬出し、VM cleanup確認後だけHost quarantineへ検証・展開してcandidate patchを回収する経路を実装した。
- [x] (2026-08-31 03:22:03Z) 現行`sbx v0.39`向けstrict decoder、sandbox lifecycle、negative probe、exact network policy、cleanup確認をsynthetic fixtureで実装した。fixture由来の結果はproduction attestationとして扱わない。
- [x] (2026-08-31 03:46:21Z) 標準service factoryから隔離brokerを配線し、router attestation失敗時はEventStore生成やmodel起動より前にfail closedするようにした。live testはopt-in、digest固定template、exact host listの全gateがない限りsubprocess前にskipする。
- [x] (2026-08-31 04:12:26Z) timeout、output limit、nonzero exitをcleanup-confirmed構造化失敗として分離し、workspaceはexportせず、bounded raw stdout/stderrとsandbox/attestationだけをwrite-once archiveへ保存する経路を実装した。
- [x] (2026-08-31 04:14:06Z) ExecPlan validator、Ruff format/check、compileall、全体pytestを同一スナップショットで通過させた。633 passed、1 live opt-in skipで、Docker sandboxおよび実modelは起動していない。
- [x] (2026-08-31 05:25:22Z) service、EventStore、broker subprocess、sandbox、agent/modelを起動しない `oracle worker readiness` を追加し、checked-in configをstable reason IDと6個のproduction evidence blockerで診断できるようにした。
- [x] (2026-08-31 05:25:22Z) Docker公式standalone `sbx v0.39.0`を導入し、Docker Sandboxes serviceのhost-side OAuth、Apple virtualization、daemon、storage、version一致、認証を `sbx diagnose -o json` の12 checksで確認し、global network policyを`deny-all`へ初期化した。
- [x] (2026-08-31 05:25:22Z) 外部modelを呼ばず、一時fixture repositoryだけを`sbx create --clone shell`へ渡す実microVM probeを行った。source mountのread-only、private clone、instance UUID、data-plane default deny、kit allow、cleanup後のsandbox消滅を実測した。
- [x] (2026-08-31 05:46:32Z) ExecPlan validator、Ruff format/check、compileall、全体pytestをreadiness統合後の同一スナップショットで通過させた。644 passed、1 live opt-in skipで、実Codex/OpenCode/Oracle modelは起動していない。
- [x] (2026-08-31 06:09:46Z) 2回のtimeout後、Humanのaction-time confirmationを受けてfresh `sbx secret set openai --oauth` flowを完了した。CLIはglobal scopeへの保存を報告し、`sbx secret ls`は`openai (oauth configured)`を返した。token値は読み出していない。
- [x] (2026-08-31 06:14:58Z) コミット前の同一スナップショットでExecPlan validator、Ruff format/check、compileall、全体pytest、`git diff --check`を再実行した。644 passed、1 live opt-in skipで、実agent/modelは起動していない。
- [x] (2026-08-31 07:05:49Z) no-model実`sbx v0.39` observationマイルストーンを開始し、strict decoder、非authorizing report、read-only archive、focused/full検証まで完了した。server-issued instance bindingとmutating cleanupは達成扱いにせず、後続security auditでv0.39のname-selected TOCTOUを閉じられないと判明したため撤去した。sandbox ownershipを含むproduction evidence不足は下記の独立した未完了blockerとして維持し、production bindとlive-agent gateを解除しない。
- [x] (2026-08-31 07:34:59Z) 実`sbx v0.39` strict decoder、非authorizing report、explicit CLI gate、0600 write-once raw archive、disposable fixture marker、UUID/name/workspace/image binding、操作・cleanup前再確認を実装し、no-model実probe `obs_3fa366faef9f4874804beb05e47073d4`を`status=observed`かつcleanup confirmedで完了した。production blockerは解除していない。
- [x] (2026-08-31 08:27:02Z) 後続security auditでname-selected cleanupのcheck-to-use race、失敗createの所有権誤認、shared skills/MCP/credential scope、runner由来偽装を発見した。実probeから`create`/`exec`/`run`/`stop`/`rm`をすべて撤去し、exact concrete subprocess runnerによるread-only `version`/`ls`/optional `inspect`だけへ縮退した。
- [x] (2026-08-31 08:27:02Z) explicit provenance edgeとargv/stdout/stderrの0600原子的archiveを実装し、read-only実probe `obs_f58ae6caf1564d198feb8d8c7cfed1b6`を空inventory、`status=observed`で完了した。manifest SHA-256は`b46838075c9851fa6744f221e921f6e1140680501daf29b5970bce2b0eee3049`で、side-effecting command、sandbox、agent/modelは起動していない。
- [x] (2026-08-31 08:32:13Z) security-focused suiteは240 passed、1 live opt-in skip、全体pytestは683 passed、1 live opt-in skipだった。Ruff format/check、compileall、ExecPlan validator、`git diff --check`も同一スナップショットで成功した。
- [x] (2026-08-31 08:36:58Z) report constructorのruntime type validationとfixed read-only argv allowlistを最終hardeningし、全体pytestを再実行して683 passed、1 live opt-in skip in 38.12sを確認した。
- [x] (2026-08-31 09:58:14Z) archive境界refactor前に指定focused baselineを実行して108 passedを確認し、対象SBX production 3 moduleを`wc -l`で2068行と記録した。baseline失敗の修正や期待値変更を混ぜずに着手した。
- [x] (2026-08-31 09:58:14Z) fixed read-only argv、report/provenance整合性、truth-domain対応、public metadata、raw artifact/manifest組立てを`sbx_observation_payload.py`へ集約した。probeは実行/decode/report orchestration、archiveはexact concrete reportから凍結済みpayloadを受け取るfilesystem commitへ縮退し、production LOCは1982行まで減少した。
- [x] (2026-08-31 10:53:07Z) final archive-boundary auditでimmutable snapshot、real issuance seal、exact UTC、canonical operation/provenance/name binding、callback secret redaction、artifact owner検査、runtime type-hint互換を追加した。focused suiteは129 passed、全体pytestは711 passed、1 live opt-in skipで、最終production LOCは2066行だった。
- [x] (2026-08-31 11:12:53Z) 公開例外のdirect `RuntimeError` MRO、安全なbuilt-in zero-offset timezoneの受理、public reason IDのsecret-carrier拒否を回復・追加し、real成功回帰を実probe `obs_f58ae6caf1564d198feb8d8c7cfed1b6`由来の明示historical fixtureへ置換した。focused suiteは131 passed、全体pytestは713 passed、1 live opt-in skip、production LOCは2066行だった。
- [x] (2026-08-31 11:34:27Z) public reason IDをexact発行候補allowlistへ変更し、provenance/argvのhostile string subclassをcallback非実行で拒否した。archive traversalのchild dirfd close保証とmode/owner検査の単一化も追加し、focused suiteは137 passed、production LOCは2066行になった。旧manifestのunknownをcurrent-schemaのfalseへ変換するテストhelperはhistorical importではなくmanual real-raw replayと明記し、source/replay hashの不一致を回帰にした。
- [x] (2026-08-31 11:41:49Z) 最終同一snapshotでfocused 137 passed、全体pytest 719 passed/1 live opt-in skip、Ruff format/check、ExecPlan validator、`git diff --check`を通過した。二系統のread-only security auditでも残存P0/P1なしを確認し、live SBX、sandbox、Codex/OpenCode、OracleProvider、外部modelは起動していない。
- [x] (2026-08-31 11:46:51Z) archive adapterに残っていたexact report typeの事前判定を削除し、任意Protocolへ触れず拒否する正本をcanonical payload builderだけにした。focused suiteは137 passedを維持し、production LOCは2063行、baseline比5行減になった。
- [x] (2026-08-31 12:03:13Z) historical Progressのno-model observation完了部分と、現在も未解決のproduction isolation blockerを分離した。過去のmutating ownership probeを成功扱いへ変更せず、残作業を特権broker/conformance、6 blocker、明示opt-in live smokeの二つの未完了項目へ整理した。
- [ ] (2026-08-31 03:46:21Z) security auditで発見したworkspace quiescence、guest Git control、data-plane network/credential、actual template instance、sandbox ownership、profile/workspace bindingの証拠不足を解消する。解消まではproduction `sbx` bindingを明示的に拒否し、実agent smokeを実行しない。

## Surprises & Discoveries

- Observation: 初期監査時の `repository_edit` はリポジトリ変更を成果物として回収しなかった。
  Evidence: 現在はrepository editを通常のHost analysis ingestionから分離し、trusted captureからcandidate patchとwrite-once archiveを生成する。

- Observation: 初期実装のlinked `GitWorktreeWorkspace` は終了時にworktreeを削除し、sourceのGit control dataも共有していた。
  Evidence: 現在の`GitStandaloneCloneWorkspace`と互換aliasはbundle経由のstandalone cloneを使い、remote、alternates、source object共有を持たない。

- Observation: 静的監査開始時は標準CLIがworker routerを構築していなかった。
  Evidence: 現在は `config/agents.toml` とdefault factory接続が追加されたが、checked-in profileはすべてdisabledで、運用pipelineの全体gateはまだ未完了である。

- Observation: 一時ディレクトリをcurrent working directoryにするだけではOSレベルのsandboxにならない。
  Evidence: `BaseAgentAdapter` はsubprocessのcwdと環境変数を制限するが、プロセス自体のファイルシステム、ネットワーク、子プロセス能力を隔離しない。

- Observation: process groupによる停止も完全な隔離境界ではない。
  Evidence: worker descendantは新しいsession/process groupを作成できる。CodexのCLI sandbox flagとOpenCode wrapper pathは、CLI本体、credential、network、Host filesystemへの到達不能性をOracle Labから機械的に証明しない。このため標準routerはcoding profileを構築前に拒否する。

- Observation: 現在の統合テストはfake executableによる契約試験であり、実Codex/OpenCode CLIの起動を検証しない。
  Evidence: `tests/test_agent_adapter_service_integration.py` は一時shell scriptをworkerとして使用する。

- Observation: 静的監査で `WorkerRouter.route()` のDirect APIからcoding agentへの暗黙フォールバックを発見した。
  Evidence: 現在の実装はDirect APIが未設定の場合に `AgentAdapterError` を送出するよう更新された。この境界をregression testで固定する。

- Observation: repository-edit taskも通常のHost分析と同じ `analysis.*` ingestion境界を通る。
  Evidence: `_execute_host_worker_job()` はjob kindに関係なく `worker.ingest()` を呼び、claimやmotifのprojectionへ到達し得る。

- Observation: 現行のjob leaseは既定60秒だが、agent timeoutは既定300秒で、実行中heartbeatがない。
  Evidence: 長時間run中にleaseが切れると、別runnerが同じtaskを再取得できる。

- Observation: 既存のDocker sandboxはHost mountを禁止しているが、repository treeを安全に搬入する専用経路を持たない。
  Evidence: `DockerShellSandbox` は有界の個別filesを一時workspaceへ展開するだけで、staging worktreeと検証結果を結び付けない。

- Observation: worker patch projection用のmigration 3が追加された直後、既存のmigration試験がv1/v2までの前提を保持していた。
  Evidence: 2026-08-30 20:59:03Z時点の全体試験は `tests/test_schema_migrations.py` の4件が失敗し、300 passed、1 skippedだった。migration番号と期待historyを更新して再検証する。

- Observation: repository-edit専用handlerの追加後、既存のservice統合試験が旧形式のjob payloadを直接enqueueし続けている。
  Evidence: `test_service_automation_routes_repository_job_to_isolated_adapter` は `repository_path` とtask eventを持たないjobを作り、専用handlerで `Path(.../None)` になって失敗した。公開enqueue APIとfixture Git repositoryを使うE2Eへ更新する。

- Observation: runtime data rootをcwd相対の`.oracle_lab`にすると、targetがcwdの場合にworker archiveやstaging親がsource fingerprintの外側でsourceへ書き込まれる。
  Evidence: default rootを外部user-data directoryへ移し、明示的なunsafe home/DB/archive/stagingをfail closedにした。full enqueue/run/archive/apply回帰はsource内に`.oracle_lab`が作られないことを確認する。

- Observation: HEAD、porcelain status、通常ファイルだけではsource不変性を証明できない。
  Evidence: `assume-unchanged`のようなindex-only変更とGitが無視するempty directory変更は旧fingerprintを通過した。現在はraw index、stage entry/mode、index flag、全非`.git` directory/file mode/contentをhashする。

- Observation: linked Git worktreeはworker隔離にならず、sourceのconfig、hooks、refs、objectsを共有する。
  Evidence: repository workspace、trusted capture、stagingをbundle経由のstandalone cloneへ変更し、Host Gitはinherited `GIT_*`、system/global config、hooks、fsmonitor、filtersを無効化した。hook/filter/config/ref/object攻撃の回帰試験が通過した。

- Observation: target preconditionをapply時だけ確認すると、既に無効なpatchをHuman candidateとして提示できる。
  Evidence: `test_conflicting_target_precondition_is_rejected_before_human_gate` はcontent drift時に `worker.patch_proposed` を生成せず、security rejectionを記録する。

- Observation: archive完成後、queue acknowledge前にcrashすると、max attempts到達済みの通常leaseではarchiveを回収できない。
  Evidence: exact task/profile/routing/prompt/argv/archiveを検証した場合だけ、同じjobへ1回限定のrecovery-only leaseを与える。再度期限切れになった場合はdead-letterとなる。

- Observation: branch pauseをautomation loop全体の停止として扱うと、unpaused sibling branchまで停止する。
  Evidence: `JobQueue.lease` のexact `(session_id, branch_id)` exclusionにより、paused branchを未leaseのまま残してsiblingを処理する。

- Observation: approvalとapply job、application eventとvalidation jobを別transactionにすると、再試行不能な中間状態が残る。
  Evidence: `tests/test_worker_transaction_recovery.py` が両組のrollbackとidempotent reconciliationを固定した。

- Observation: bundle内の過去のHuman approvalやpending jobをそのまま復元すると、importがlocal filesystem execution authorityになり得る。
  Evidence: imported worker chainを `historical_only` / `imported_historical` とし、pending jobsをcancelled quarantineへ移す試験が通過した。

- Observation: worker actorだけを除外しても、workerを祖先に持つHost派生物やHuman keepを通じて研究projectionを汚染できる。
  Evidence: worker lineageを推移的にclaims、motifs、curation、selected corpusから除外し、`approver_event_id`を含むsynthetic ancestryもprojection rebuild時に追跡する。

- Observation: queue lease ownerを省略可能にすると、別runnerがactive leaseをcompleteまたはfailできる。
  Evidence: terminal transitionはnonblank ownerを必須とし、Python確認とSQL更新条件の両方でstale runnerをfenceする。runごとの固有ownerをheartbeatからterminal acknowledgeまで使う。

- Observation: validation対象をindexから複数Git commandで読むと、確認済みtreeとDockerへ渡すbytesの間にTOCTOUが生じる。
  Evidence: validationは承認済み`target_tree`のtree/blob OIDから直接snapshotを作り、各OIDを再計算する。Storeはarchive task/command/metadataとterminalの全identityを照合し、applicationごとのterminalを一意化する。

- Observation: event payloadのorigin labelだけではHost文をgenuine Oracleとして偽装できる。
  Evidence: non-synthetic `oracle.output` はMODEL actor、実在request/context/provider ancestry、archive sidecarと完全なmodel identityをStoreで要求し、selected corpusでもactor/originを再確認する。

- Observation: Direct HostはOracleProviderと同じmodel registryへ暗黙接続するとoriginとprompt contractを混同する。
  Evidence: Direct Hostは`config/agents.toml`の明示的なHost専用provider/model設定だけを使い、raw provider envelopeとactual identityを`host_generated` archiveへ保存する。`models.toml`と`oracle.output`経路は使用しない。

- Observation: Direct Hostのprovider responseを通常のbuffered HTTP読取りへ任せると、worker profileのoutput limitを越えるmemory/archive消費を防げない。
  Evidence: transportをstreaming hard boundにし、超過時はbounded prefix、`output_limited=true`、failure terminalだけを保存する回帰を追加した。

- Observation: credential header名のdenylistだけでは、providerが別名のresponse headerやbodyへcredential値を反射した場合にarchiveへ残る。
  Evidence:既知credential値によるheader redactionを行い、bodyに値が含まれる場合はraw responseを保存せず`quarantined_credential`としてfail closedする。

- Observation: 通常の`human.keep`をclaim固有のcanon approvalとして流用できると、別claimのcanonical promotionをStoreへ直接appendできる。
  Evidence: canonical promotionはcandidate、claim、Human approval、session、branch、target、causationをStore境界で完全に結合し、actor種別に関係なくshortcutを拒否する。

- Observation: macOS上のDocker Sandbox v0.12.0はmicroVM型のCodex/OpenCode backend、sandbox単位のdefault-deny network proxy、およびHost側で実credentialを注入する`proxy-managed`認証経路を持つ。
  Evidence: local `docker sandbox version/create/network proxy` help、Apple署名済みplugin identity、binaryのCodex proxy-managed設定を確認した。availabilityやhelp文字列だけはattestationとせず、実backend conformanceをrouter activation条件にする。

- Observation: ExecPlanのfocused-test commandは現在存在しない`tests/test_agent_patch_pipeline.py`と`tests/test_agent_patch_security.py`を参照していた。
  Evidence: 元commandはcollection前にexit 4となった。現行の対応ファイル`test_agent_adapter_repository_edit.py`と`test_candidate_patches.py`へ置き換えたbaselineは53 passedだった。

- Observation: ローカルの旧`docker sandbox v0.12.0`は、read-only probeで指定した`--pull-template never`に反してtemplate pullを開始した。
  Evidence: pull開始を確認した時点でprobeを中断し、作成途中のsandboxをcleanupした。`docker sandbox ls`で残存sandboxなしを確認したため、この旧pluginをproduction backendとして有効化しない。

- Observation: 現行`sbx`の`cp`はHost側のhard byte/entry limitを提供せず、private cloneの停止時snapshot契約も文書化されていない。
  Evidence: workspaceを単一のversioned length-frameへguest側でstreamし、Hostのbounded subprocess capture、raw byte limit、entry limit、regular payload limit、strict parserをすべて通した後だけmaterializeする専用境界へ変更した。

- Observation: capability名の列挙だけをattestationとして受け入れると、実際に検査していない性質を一つのgeneric checkで偽装できる。
  Evidence: receipt schema、timezone-aware timestamp、unique check ID、および宣言した全capabilityと同名のpassing checkを`IsolationAttestation` constructorで必須化した。

- Observation: worker本体の終了はworkspaceの静止を意味しない。
  Evidence: detached descendantはmain process終了後もprivate cloneを変更できるため、worker終了直後に別`sbx exec`でarchiveを作る実装では最終treeを再現できない。timeout、output limit、nonzero exitではexportせずcleanup-onlyにし、成功時も全descendant quiescenceを実測できるまでproduction bindを拒否する。

- Observation: ordinary workspace archiveからroot `.git`を除外すると、guest内のcommit、config、hook、ref、object改変をHost側のclone比較では観測できない。
  Evidence: brokered adapterが比較していた`.git`はcleanup後にordinary filesだけを受け取るHost cloneのもので、guest Git controlではなかった。guest pre/post Git receiptを信頼境界内で作れない限りproduction evidenceは不完全である。

- Observation: 現行`sbx v0.39`の`template ls --json`実出力はregistry digestを公開しない。
  Evidence: image ID/tagをdigestの代替として推測せず、明示的な`synthetic_fixture`だけでparser成功系を試験し、production inventoryはfail closedする。

- Observation: Linux guest、policy CLI、環境sentinel、sandbox名の消滅だけでは、microVM/data-plane enforcement、credential proxy非開示、Host Docker非到達、全descendant破棄を個別に証明し切れない。
  Evidence: security auditは宣言capabilityが測定値より強いことを発見した。actual data-plane、runtime/template instance、credential proxy、quiescenceの証拠を追加するまでpassing production receiptを発行しない。

- Observation: failed workspaceを捨てるだけでは、timeout直前までのbounded stdout/stderrという監査資料まで失われる。
  Evidence: `IsolationRunResult`を成功＋export専用にし、失敗はcleanup-confirmed `IsolationRunFailed`としてraw streams、argv、sandbox ID、attestationだけを運ぶ。adapterはfailed `AgentRunResult`へ変換し、patch/export fieldsを空のままarchiveする。

- Observation: 実行前のbroker/template/policy identity検査だけでは、run中の差し替えを古いattestationで受け入れ得る。
  Evidence: sandbox cleanup確認後、成功結果または構造化失敗を返す直前にexecutable hash、client/server version、template inventory identity、global policyを再測定し、drift時は結果を受理しない。

- Observation: Typer root callbackで全commandの前に`OracleLabService.default()`を生成すると、readiness診断だけでもruntime home、DB、router bindへ進む。
  Evidence: service生成を最初のservice-backed commandまで遅延し、`oracle worker readiness`がservice factory、subprocess、broker bindを呼ばない回帰試験を追加した。

- Observation: 実standalone `sbx v0.39.0`のcontrol-plane出力は、staged synthetic fixtureの同版grammarと一致しない。
  Evidence: 実`sbx version`は単一の`sbx version: v0.39.0 <commit>`行であり、global policy JSONは`policy_name`、sandbox policy JSONは`policy_id`に加えて`origin=scoped`と`sandbox_id`を返した。現fixtureのclient/server二行grammarとstrict policy decoderをproduction evidenceへ昇格できない。

- Observation: global `deny-all`はbuilt-in kitのsandbox-scoped allowを消去する設定ではない。
  Evidence: no-model shell fixtureのeffective policyは`openrouter.ai` allowを含み、guest data-planeで`https://example.com`はproxyの403/default deny、`https://openrouter.ai`は200となった。exact allowlistはglobal preset名ではなくcreated sandboxの実policyと通信結果で検証する必要がある。

- Observation: 実`sbx create --clone`はsourceを`/run/sandbox/source`へread-only mountし、private cloneとinstance UUIDを作るが、`sbx ls --json`はtemplate identityを返さない。
  Evidence: guest-only fileとlocal Git configはHost fixtureへ現れず、cleanup後はsandboxと一時remoteが消滅した。一方、template inventoryはshort image ID、repository、tag、flavor、時刻、sizeだけでregistry digestを持たず、instance一覧にもtemplate fieldがなかった。

- Observation: `sbx diagnose`全check成功はHost前提の確認であり、Oracle Labのproduction attestationではない。
  Evidence: 実機では12 checksがpassしたが、workspace quiescence、guest Git control receipt、credential non-disclosure、instance template digest、ownership race、profile/input bindingは診断対象に含まれない。

- Observation: Docker Sandboxes serviceのrefresh tokenは一度`invalid_grant`を返したが、明示的な`sbx login`後は公式diagnosticが再び12 passed、0 failed、0 skippedになった。
  Evidence: 失効観測と再ログインを分離して記録し、OpenAI service secretの存在をDocker service login成功の代替にしなかった。

- Observation: 実`sbx v0.39.0`には`sbx inspect SANDBOX_NAME --json`があり、agent、state、workspace、network policy、secrets、active sessionsをsandbox単位で観測できる。一方、空inventoryの`sbx ls --json`は`{"sandboxes":[]}`、`template ls --json`はshort image IDだけを返した。
  Evidence: 再ログイン後のHost control planeからhelpとraw JSONを取得した。実sandboxのinspect schemaとinstance UUID/image identityはno-model probeで追加観測する。

- Observation: credentialを継承しない最小環境から`sbx create --clone`を起動する場合も、`sbx`内部のHost Git探索には固定`PATH`が必要だった。
  Evidence: 初回の実CLI probe `obs_b55836bfafa349f1b9c29dce26c7173b`はsandboxを作らず`sbx_probe_create_failed`で停止し、raw stderrを非公開archiveへ保存した。任意operator PATHを継承せず`os.defpath`を渡す修正後、同じfixture classでcreateに成功した。

- Observation: 実`inspect --json`はtag形式のrequested imageと完全な`image_digest`を返し、`ls --json`はserver UUIDを返すが、name-selected commandはUUIDをselectorとして受け付けない。
  Evidence: 最終probeはname、UUID `5d3c35c1-08d3-4298-8f36-1032594fe239`、workspace、image digestをcreate後とcleanup直前に再確認し、削除後inventoryを空として観測した。別のread-only試行でUUIDを`inspect` selectorへ渡すとnot foundになった。

- Observation: name/UUIDを`ls`と`inspect`で再確認してから`rm NAME`しても、確認と削除の間にnameが別UUIDへ再利用されると別sandboxを削除できる。
  Evidence: v0.39のmutation CLIはserver UUID/CAS tokenを受け付けることを証明できず、create成功出力もserver UUIDを因果的に返さない。高entropy name、成功exit、前後inventoryはこのcheck-to-use gapを閉じない。

- Observation: read-only `ls`とname-selected `inspect`を2回ずつ成功させても、UUIDとinspect fieldsを同一instanceへ原子的に結べない。
  Evidence: ABAで`ls`がinstance AのUUID、`inspect`が同名instance Bのworkspace/imageを返し、次の組も同じ交互順になる可能性がある。agent/status/workspaceが一致しても架空のcomposite identityを排除できないため、derived identity型を撤去した。

- Observation: `no-model shell`というagent選択だけではsandbox startupの共有状態を空にできない。
  Evidence: 実inspectはMCP gateway/secret referenceを返し、v0.39にはshared skills、global service credentials、built-in network allowancesがある。専用scopeとempty credential/MCP/skills/network境界を起動前に証明できないため、観測コードからsandbox作成自体を撤去した。

- Observation: injected runnerの自己申告文字列をtruth-domainの根拠にするとsynthetic bytesを`real`として保存できる。
  Evidence: arbitrary runnerの`evidence_origin=production`を拒否し、exact concrete `SubprocessCommandRunner`だけが`truth_domain=real`を生成できる回帰を追加した。returned argvもrequested argvと完全一致しなければ受理しない。

- Observation: hardened archiveの初回Host実行はfilesystem sandboxのEPERMをstaging collisionと誤分類した。
  Evidence: home archiveはowner一致・0700・staging残骸なしで、同一処理は`/private/tmp`で成功した。staging mkdirのerrno分類を修正し、escalated Host実行では同じhome archiveへの原子的commitに成功した。

- Observation: arbitrary Protocol reportを保存前に検証して拒否する実装でも、拒否理由を分類するために`to_public_dict()`や属性へ触れると、その時点で任意Python side effectを実行できる。
  Evidence: independent refactor auditでpayload builderのlegacy rejection pathを反証した。archiveとbuilderの最初の判定をexact `SbxNoModelObservationReport` type checkへ変更し、exploding Protocolの`to_public_dict()`が一度も呼ばれない回帰を追加した。

- Observation: report constructorとarchive直前に同じ安全条件を別実装すると、validation回数を維持していても条件集合とreason mappingがdriftする。
  Evidence: refactor前はread-only argv、status/origin/truth、provenance completeness、public key set、raw hash/sizeがprobe/report/archiveへ重複していた。現在は同じpayload validatorを生成時とarchive直前に再実行し、object-level tamperを二境界で検出する。

- Observation: frozen dataclassも`object.__setattr__`や並行処理では発行後に変更できるため、seal検証後に元reportを再参照するとraw artifactとmanifestを別状態から組み立て得る。
  Evidence: archive入口でversion、observations、provenanceをexact concrete型へ複製し、そのsnapshot自身の共通validatorとreal sealを検証して以後はsnapshotだけを読む。最初のhash計算時に元reportを変更しても変更前raw bytesが保存される回帰で固定した。

- Observation: exact `datetime`だけではstateful custom `tzinfo` callbackを排除できず、provenanceの集合一致だけではsource入替え、edge順序、別sandbox名の二つのinspectを拒否できない。
  Evidence: exact built-in `datetime.timezone`のzero-offsetだけを許し、custom callback timezoneを非実行で拒否する。canonical operation prefix、derived-fieldごとのexact source tuple/order、inspect名一致も単一validatorとtamper回帰で固定した。

- Observation: 公開型のimport/pickle identityを保つための`__module__`付替えは、移動先moduleにannotation名がなければ`typing.get_type_hints()`を壊す。
  Evidence: probe/archive側へ必要なannotation名を明示再exportし、report、observation、legacy Protocolとpublic methodのtype-hint解決を回帰試験にした。

- Observation: 0600 mode確認だけではartifact owner検査にならない。
  Evidence: raw artifactとmanifestの各open descriptorで`st_uid == geteuid`も検証し、foreign-owner statを注入した場合はcanonical archiveを残さずfail closedする。

- Observation: regex形状だけを検査したpublic `reason_id`はsecret-like文字列のcarrierになり得て、LOC削減用のprivate exception baseは公開例外のMROを変える。
  Evidence: reason IDを`sbx_probe_*`または`sbx_v039_*`へ限定してcredential/password/private/secret/token類を拒否し、公開三例外が直接`RuntimeError`を継承することを回帰試験にした。

- Observation: forbidden-word regexを追加しても`ghp_*`や`sk_live_*`形式のsecret-like値はpublic `reason_id`を通過でき、tuple equalityだけのprovenance検査は`str` subclassのcallbackを実行できる。
  Evidence: incomplete reportが受理できるreason IDをfixed operation failure、strict decoder failure、明示fixture reasonを含むexact集合へ限定し、非発行IDとhostile provenance subclassがarchive root作成前にsecret非露出で拒否される回帰を追加した。read-only argv正本もtuple equality前に全要素のexact `str`型を検査し、mutating文字列を偽装するsubclassをrunner callback非実行で拒否する。

- Observation: `obs_f58ae6caf1564d198feb8d8c7cfed1b6`の保存済みmanifestは`name_selected_inspect_observed`と`atomic_instance_binding_proven`をunknownの`null`で保持していたが、current-schema test helperは両方を`false`にして別hashを生成していた。
  Evidence: 旧manifest hash `b46838075c9851fa6744f221e921f6e1140680501daf29b5970bce2b0eee3049`とcurrent-schema replay hash `ab54d0fc4548d44c3520cd9d612e11380549289ad835875f92545dea902e969b`を区別し、このhelperをhistorical session importと呼ばない回帰へ変更した。旧archive自体は変更していない。

- Observation: archive root traversalはopen済みchild dirfdの`fstat`失敗時に親だけをcloseし、childを漏らし得た。
  Evidence: 親dirfdをchildへ引き継ぐ前にcloseし、以後の失敗はouter `finally`相当経路がcurrent childをcloseする順序へ変更した。失敗注入で二つのdescriptorが閉じられることを固定した。

- Observation: archive adapterとcanonical payload builderの双方にexact concrete report判定が残り、同じ入力を同じreasonで拒否していた。
  Evidence: builderは型不一致時にProtocol属性やmethodを一切評価せずstable payload errorを返すため、adapter側判定を除去してもarbitrary Protocol非実行回帰と公開reasonは不変だった。

## Decision Log

- Decision: コーディングエージェントは常に `worker` actorとして記録する。
  Rationale: Oracle、Host、Human、Toolの由来を永久に区別可能にするため。
  Date/Author: 2026-08-30 20:45:18Z / Initial plan

- Decision: Host worker機能はデフォルト無効とし、設定で明示的に有効化する。
  Rationale: エージェント起動には費用、外部通信、ファイル操作が伴うため。
  Date/Author: 2026-08-30 20:45:18Z / Initial plan

- Decision: repository editの成果物は直接適用せず、不変なcandidate patchとして保存する。
  Rationale: エージェントの判断と人間の採用判断を分離し、再現、監査、拒否を可能にするため。
  Date/Author: 2026-08-30 20:45:18Z / Initial plan

- Decision: 人間の承認後も、現在の作業ツリーではなくOracle Lab管理下のstandalone staging cloneへ適用する。
  Rationale: ユーザーの未commit変更や現在の作業を破壊しないため。
  Date/Author: 2026-08-30 20:45:18Z / Initial plan

- Decision: Direct APIが未設定の場合、軽量タスクを暗黙的にコーディングエージェントへフォールバックしない。
  Rationale: 費用と実行能力が異なるworker classを暗黙的に切り替えないため。
  Date/Author: 2026-08-30 20:45:18Z / Initial plan

- Decision: エージェントによるcommit、push、merge、keep、star、canonizeを禁止する。
  Rationale: 外部状態の変更と価値判断を明示的な人間操作に限定するため。
  Date/Author: 2026-08-30 20:45:18Z / Initial plan

- Decision: workerが生成したpatchと文章をOracle context、oracle corpus、claim historyへ自動投入しない。
  Rationale: worker-generated materialとoracle-generated materialの境界を維持するため。
  Date/Author: 2026-08-30 20:45:18Z / Initial plan

- Decision: candidate patchはtrusted Hostによる決定論的security preflightに合格した後だけHuman gateへ送り、承認後にpersistent standalone staging cloneへ適用し、その後にsandbox validationを実行する。
  Rationale: 静的なpath・hash・base・mode検査、人間の採用判断、リポジトリ状態への適用、実行結果の観測を混同しないため。
  Date/Author: 2026-08-30 20:59:03Z / Static audit

- Decision: `repository-edit` と `prompt-oracle` は別のcapability、job kind、event経路とする。repository-edit workerはOracleの出力を模倣せず、Oracle contextへ文章を挿入せず、Oracle呼び出しを暗黙に起動しない。
  Rationale: coding-agent artifactとoracle-generated materialの起源を永久に分離し、Oracleが必要な場合はconfigured `OracleProvider`とexact prompt archiveを通すため。
  Date/Author: 2026-08-30 20:59:03Z / Static audit

- Decision: live-agent testは既定でsubprocess起動より前にskipする。OSレベルのworker隔離とfixture repository pipelineが完成するまで、opt-inがあっても実CLI起動は安全にskipする。
  Rationale: opt-inフラグの追加だけで、未隔離のCodex/OpenCodeをHost上で実行しないため。
  Date/Author: 2026-08-30 20:59:03Z / Static audit

- Decision: 標準config/default serviceは、Codex/OpenCodeの`enabled=true`をOS分離backendとして扱わず、router構築中にfail closedする。明示注入fake adapterだけを決定論的契約試験に使用する。
  Rationale: Humanが設定を管理していてもworker/modelは非信頼であり、CLI flag、wrapper path、cwd、環境allowlist、process-group killは特権隔離brokerの代替にならないため。未実装backendを設定文字列だけでattestできるescape hatchも設けない。
  Date/Author: 2026-08-31 / OS isolation closure

- Decision: worker control dataはtarget repositoryとCodexを起動したcurrent Git worktreeの外に置き、unsafeな明示overrideは自動補正せずfail closedにする。
  Rationale: archive、SQLite、workspace、stagingの通常書込みが、agent実行前後のsource fingerprintを迂回してsourceを変えないようにするため。
  Date/Author: 2026-08-31 / Final security audit

- Decision: repository-edit、trusted patch capture、stagingにはlinked worktreeを使わず、bundle経由のsource-independent standalone cloneを使う。
  Rationale: worker-written Git config、hooks、refs、objectsをHostが解釈せず、source repositoryとのobject共有やpush経路を持たせないため。
  Date/Author: 2026-08-30 23:03:09Z / Final integration

- Decision: worker execution profile、routing選択、validation command、sandbox limitsとimageをtask/jobへfreezeし、現在設定とのdriftをprocess起動前に拒否する。
  Rationale: profile名だけでは実行条件を再構築できず、再試行時の設定変更が同一taskの意味を変えるため。
  Date/Author: 2026-08-30 23:03:09Z / Final integration

- Decision: target repositoryのbase、content、mode preconditionはHuman gate前とapply前の両方で検査する。
  Rationale: 採用不能なcandidateをHumanへ提示せず、承認後の競合も自動解決しないため。
  Date/Author: 2026-08-30 23:03:09Z / Final integration

- Decision: approval＋apply enqueue、およびapplication＋validation enqueueをそれぞれ単一transactionにする。
  Rationale: authoritative eventだけ、またはjobだけが残る中間状態を防ぐため。
  Date/Author: 2026-08-30 23:03:09Z / Final integration

- Decision: exact archiveが検証できたlease expiryに限り、同じjobへ1回だけrecovery-only leaseを与える。
  Rationale: agentやDockerを再起動せずarchiveから継続しつつ、通常のretry budgetを迂回しないため。
  Date/Author: 2026-08-30 23:03:09Z / Final integration

- Decision: imported worker eventsとapprovalはimmutable historical evidenceとし、local approval、apply、validation authorityを持たせない。
  Rationale: replay portabilityとlocal filesystem capabilityを分離するため。
  Date/Author: 2026-08-30 23:03:09Z / Final integration

- Decision: approval/application orchestrationは`OracleLabService`のstore/queue transaction内に置き、`patches.py`は純粋なcandidate検証・適用helperを持つ。
  Rationale: eventとjobの原子性を共有し、worker出力からHuman judgmentまたはapplicationを直接起動できないようにするため。
  Date/Author: 2026-08-30 23:03:09Z / Final integration

- Decision: Direct API HostはOracleProvider/model profileを再利用せず、明示的なHost専用provider/model/sampling設定と`host_generated` archiveを使う。
  Rationale: Host analysisのprompt、provider identity、usage、raw responseを監査可能にしつつ、Oracle materialと永久に区別するため。
  Date/Author: 2026-08-30 23:03:09Z / Final integration

- Decision: Direct Hostのresponse size、fallback policy、credential非永続化をtransport境界で強制し、archiveとterminalでも同じdispositionを再検証する。
  Rationale: 設定を記録するだけではmemory bound、routing intent、secret isolationの実効性を証明できないため。
  Date/Author: 2026-08-30 23:13:28Z / Final boundary audit

- Decision: coding workerのproduction isolation候補は現行Docker `sbx` microVMに限定し、旧`docker sandbox` plugin、Codex自身のsandbox flag、OpenCode wrapper pathを同等のattestationとして扱わない。
  Rationale: Host filesystem、Host Git、credential、network、descendant cleanupを同一の機械検証receiptへ結び付け、利用可能性やhelp文字列だけで安全性を推定しないため。
  Date/Author: 2026-08-31 03:22:03Z / Isolation broker implementation

- Decision: global network policyを自動変更せず、Locked Down相当かつactive global/org allowがない場合だけsandbox-scoped exact host allowを追加し、実行前後のeffective policy完全一致を要求する。
  Rationale: Oracle Labの起動が他のsandboxの通信権限を変更せず、Balanced presetやwildcard ruleから意図しない外向き通信能力を継承しないため。
  Date/Author: 2026-08-31 03:22:03Z / Isolation broker implementation

- Decision: VMからHostへ戻すworkspaceはOracle Lab Workspace Archive v1のopaque bytesだけとし、cleanup確認前の展開、`sbx cp`によるdirectory copy、自己申告hash/counterを禁止する。
  Rationale: untrusted filesystemのsymlink、hardlink、special file、path collision、容量爆発、TOCTOUをHost Git control planeへ到達する前に閉じるため。
  Date/Author: 2026-08-31 03:22:03Z / Isolation broker implementation

- Decision: synthetic `sbx` protocol fixtureはparser/lifecycle testにだけ使用し、receipt内で`synthetic_fixture`と表示する。production subprocess runnerは不足しているquiescence、Git-control、data-plane、template-instance証拠が揃うまでattestationを発行しない。
  Rationale: fixtureが期待したJSONを返した事実を実machineの隔離能力へ昇格させず、未測定capabilityをpassing checkとして偽装しないため。
  Date/Author: 2026-08-31 03:46:21Z / Post-implementation security audit

- Decision: live smokeのopt-inだけではworker起動権限にしない。digest固定template、exact host list、broker identity、complete production conformanceをすべて満たした後だけfixture repositoryへ進む。
  Rationale: 外部呼び出しへの同意とOS isolationの証明は別のgateであり、片方からもう片方を推定できないため。
  Date/Author: 2026-08-31 03:46:21Z / Live gate revision

- Decision: 成功したisolated runだけがworkspace exportを持てる。timeout、output limit、nonzero exitはcleanup確認後にraw stdout/stderrを保存するが、workspace、patch、Human gateを生成しない。
  Rationale: 失敗treeの非原子的な取り込みを防ぎながら、失敗原因のexact bounded observationは再現可能な監査資料として残すため。
  Date/Author: 2026-08-31 04:12:26Z / Failure archive hardening

- Decision: sandbox cleanupだけでなく、cleanup後のbroker executable、client/server、template、global policy identityも成功・失敗結果の共通postconditionとする。
  Rationale: 実行前に固定したattestationと、実際にrunを完了したruntime identityのTOCTOUを閉じるため。
  Date/Author: 2026-08-31 04:12:26Z / Post-cleanup identity audit

- Decision: coding-worker readinessはread-only control-plane observationとし、service/EventStore生成、profile有効化、production attestation発行、sandbox作成、agent/model起動を行わない。
  Rationale: 実行可能性の観測を実行権限や隔離証明へ昇格させず、未完成のproduction bindingを安全に診断するため。
  Date/Author: 2026-08-31 05:25:22Z / Operational readiness

- Decision: static readiness、明示承認されたno-model実`sbx` conformance、外部modelを伴うlive smokeを独立した三つのgateにする。
  Rationale: ローカル準備、OS隔離の実測、外部通信と費用へのHuman同意は相互に代替できないため。
  Date/Author: 2026-08-31 05:25:22Z / Operational readiness

- Decision: installed `sbx`とHost診断が成功しても、実CLI protocolと6 blockerを解消するまではchecked-in worker設定とproduction guardを変更しない。
  Rationale: availability、ログイン、preset名、synthetic fixtureを未測定capabilityの証明として扱わないため。
  Date/Author: 2026-08-31 05:25:22Z / Operational readiness

- Decision: 実`sbx`の部分観測は`IsolationAttestation`と別の非authorizing report型へ保存し、全capabilityが証明されるまで`status=passed`、`ready=true`、profile有効化を表現できないようにする。
  Rationale: 実control-plane schemaの学習とproduction実行権限を型レベルで分離し、不完全な観測をattestationへ昇格させないため。
  Date/Author: 2026-08-31 07:05:49Z / Real observation milestone

- Decision: UUID/name bindingの観測とcleanup確認に成功しても、v0.39の操作がname selectorとidentity checkの間を原子的に結ばない限り`sandbox_ownership` blockerを解除しない。
  Rationale: 高entropy nameと前後確認は競合を検出できるが、checkとname-selected `rm`の間のTOCTOUを機械的に不可能とは証明しないため。
  Date/Author: 2026-08-31 07:34:59Z / Real ownership observation

- Decision: no-model observation APIからsandbox lifecycle mutationを撤去し、既存control planeへのread-only operationだけを許可する。
  Rationale: UUID/CAS selectorと専用empty runtime scopeがない状態で自動cleanupを継続すると、別sandboxの誤削除またはHost-managed stateへの不要な到達を起動前に排除できないため。
  Date/Author: 2026-08-31 08:27:02Z / Post-probe security audit

- Decision: real truth domainは内部生成したexact concrete subprocess runnerだけが発行し、version、inventory、name-selected inspectの各derived fieldをsource operation IDへ明示的に結ぶ。
  Rationale: runnerの自己申告をprovenanceとして信頼せず、raw argv/stdout/stderrからderived observationを再構築可能にするため。
  Date/Author: 2026-08-31 08:27:02Z / Observation provenance hardening

- Decision: sbx observation archiveはdirfd/O_NOFOLLOWでancestorを固定し、0600 staging artifactsをfsyncした後にno-replace atomic renameでcommitする。
  Rationale: raw tool outputがsymlink raceで別pathへ書かれたり、partial directoryがcanonical archiveとして見えたり、同じprobe IDが上書きされたりすることを防ぐため。
  Date/Author: 2026-08-31 08:27:02Z / Observation archive hardening

- Decision: SBX observationのsemantic contractとcanonical archive payloadは`sbx_observation_payload.py`を単一正本とし、report生成時とarchive直前の両方で同じvalidatorを呼ぶ。
  Rationale: defense layerを減らさず、read-only argv、truth domain、provenance、public metadata、raw hash/size、manifest layoutの実装重複だけを除去するため。
  Date/Author: 2026-08-31 09:58:14Z / Archive boundary refactor

- Decision: archive入口はProtocolの自己申告を評価せず、exact concrete reportだけをcanonical immutable payloadへ変換する。`worker_archive.py`と`validation_archive.py`への共通化は行わない。
  Rationale: arbitrary method executionを拒否前に起こさず、異なるarchiveのsecurity semanticsを見た目だけで共通化しないため。既存のdirfd/O_NOFOLLOW、mode、fsync、no-replace処理は独立したFS境界として維持する。
  Date/Author: 2026-08-31 09:58:14Z / Archive boundary refactor

- Decision: archive直前にreportのnested evidenceをexact concrete immutable snapshotへ固定し、real sealをsnapshot上で再検証してからpublic manifestとraw artifactsの両方を同じsnapshotだけから生成する。
  Rationale: validationとserialization間のobject mutation、ABA、stateful callbackによる状態分裂を閉じ、生成時とarchive時の二つの防御境界を共通validatorのまま維持するため。
  Date/Author: 2026-08-31 10:53:07Z / Final archive-boundary audit

- Decision: real reportのseal keyとauthority tokenはclosure外へ公開せず、exact internal `SubprocessCommandRunner`経路だけがprivate issuerを呼ぶ。同一Python process内のprivate実装へ到達できるcodeはtrusted boundary内とし、注入runnerの自己申告は引き続きsynthetic以外へ昇格させない。
  Rationale: arbitrary Protocol、fake runner、直接constructorからreal truth domainを発行できないようにしつつ、Python process内のreflection耐性を偽のsecurity claimにしないため。real成功回帰にはmanifest SHA-256 `b46838075c9851fa6744f221e921f6e1140680501daf29b5970bce2b0eee3049`で保存済みの実probe raw/identity metadataを明示historical fixtureとして使い、テスト生成executableのbytesをrealへ昇格させない。
  Date/Author: 2026-08-31 10:53:07Z / Real evidence issuance boundary

- Decision: 上記real成功回帰の入力はimmutable historical session importではなく、保存済みreal raw/identity metadataから手動再構築したcurrent-schema replayとして扱う。旧manifestのunknownを補完した事実を隠さず、二つのmanifest hashを同一視しない。
  Rationale: historical importのmissing metadataを発明せず、real raw seal経路のoffline回帰と旧archiveの不変な履歴を区別するため。テスト用replayはcanonical corpusへ投入せず、live SBXを再実行しない。
  Date/Author: 2026-08-31 11:34:27Z / Historical unknown preservation

- Decision: public `reason_id`は形状や禁止語ではなく、probeが発行し得るexact reason集合と明示fixture reasonだけを受理する。archive用error分類集合はreportへ発行可能な集合と同一視しない。
  Rationale: stable reason ID欄をraw/credentialのcarrierにせず、report生成時とarchive snapshot再検証時で同じ閉じた規則を使うため。
  Date/Author: 2026-08-31 11:34:27Z / Public metadata boundary

- Decision: archive inputのconcrete type trust判定はcanonical payload builderだけが所有し、filesystem adapterはその検証済みimmutable payloadだけを保存する。
  Rationale: archive rootへ触れる前のfail-closed順序とProtocol callback非実行を保ったまま、同一条件の二重実装を残さないため。
  Date/Author: 2026-08-31 11:46:51Z / Concrete archive input boundary

- Decision: operation順序、derived-field順序とsource tuple、二つのinspectのsandbox名はcanonical contractの一部とし、artifact fileのmodeとownerを同じdescriptorから検査する。
  Rationale: field単体が安全でも並べ替えやcross-field substitutionで別の意味へ変わること、およびmodeだけでは別ownerのartifactを排除できないことをarchive前に閉じるため。
  Date/Author: 2026-08-31 10:53:07Z / Cross-field and filesystem hardening

## Outcomes & Retrospective

このセクションは各マイルストーン終了時に更新する。完了時には、標準CLIから
明示的に選択したCodex/OpenCodeへタスクを送り、candidate patchを確認し、人間承認後に
standalone staging cloneへ適用し、sandbox test結果までイベント履歴から再構築できるかを、
当初の目的と比較して記録する。

実装中に発見したCLI固有の出力形式、sandbox制約、認証方法、patch互換性、実行時間、
未解決の安全上の制約もここへ追記する。

2026-08-30の静的監査時点では、汎用worker adapter、durable job queue、Human actor境界、
Docker sandboxに加え、disabled-by-defaultのworker設定、worker archive、patch event/projection、
承認serviceの骨格が追加されつつある。一方で、candidate patchの決定論的preflightから
persistent staging apply、sandbox validationまでのE2Eは未完了である。READMEと
live-test骨格の追加は安全境界を先に固定するものであり、実agent連携の完了を意味しない。

2026-08-30 23:03:09Z時点で、明示注入fake workerによるdurable E2E、write-once
worker/validation archive、trusted candidate preflight、Human-only gate、source-independent
staging clone、sandbox validation、projection rebuild、CLI、bundle portability、atomic
transaction、bounded orphan recovery、branch pause、lease heartbeat、worker-lineage隔離を実装した。
source repositoryとcurrent worktreeはHEAD、index、filesystem、Git control dataを含めて不変であり、
Host Gitはworkerまたはsourceが設定したhook、filter、config、refを実行しない。Direct API Hostは
Host専用設定から標準接続でき、Oracle materialではなく`host_generated`としてarchiveされる。

一方、標準configから実Codex/OpenCodeを運用する最終成果は未達である。特権OS isolation brokerが
存在しないため、`build_worker_router`はenabled coding profileをsubprocess開始前にfail closedし、
live-agent testはoperator opt-in後もskipする。再開条件はfilesystem、network、credentials、全
descendant、Host Git control dataを機械的に隔離するbackendと、そのconformance testである。
したがってfake pipelineの完成を実agent operational integrationの完成とは記録しない。

2026-08-31 03:46:21Z時点で、isolation attestation型、strict `sbx v0.39`
protocol decoder、bounded Workspace Archive v1、cleanup後だけのHost quarantine import、標準service
factory配線、operator-only live gateを実装した。これにより、disabled標準設定は副作用なく動き、
有効化されたcoding profileもconformanceなしにはEventStoreやmodelへ進まない。

失敗経路はworkspace exportとraw process observationを分離した。timeout、output limit、nonzero exitでは
sandbox cleanupとruntime identityの再確認後に、bounded stdout/stderr、argv、sandbox ID、attestationを
failed worker archiveへ保存する。export hash、candidate patch、Human gateは作らない。cleanupまたはidentity
確認自体が失敗した場合は、構造化worker結果として受理せずhard failureにする。

ただし、synthetic fixtureによるlifecycle成功はproduction OS isolationの成功ではない。post-implementation
security auditで、detached descendantが生存した状態のexport、guest `.git`改変の不可視性、CLI policyと
sentinelだけによるdata-plane/credential capabilityの過大宣言、template instance identity、sandbox
ownership race、profile/workspace under-bindingが見つかった。安全側の停止点としてproduction bindingを
再び明示的fail closedにし、実Codex/OpenCode smokeは未実施のまま残す。当初の最終目的はまだ未達である。

2026-08-31 05:25:22Z時点で、初回実行に必要なstatic configとbroker file identityを副作用なく列挙する
`oracle worker readiness`を追加した。Hostにはstandalone `sbx v0.39.0`を導入し、Docker Sandboxes
serviceのOAuth、virtualization、daemon、storage、version、authenticationの公式diagnosticは全件passした。
global network policyは`deny-all`で初期化し、no-model shell microVMを一時fixture repositoryだけで
作成・削除した。Codex用OpenAI proxy credentialのOAuthは同意前timeoutにより未設定で、tokenは保存されていない。

この実probeによりsource read-only mount、private clone、data-plane default deny、sandbox UUID、cleanupは
観測できたが、同時に実CLI grammarとsynthetic decoderの差、built-in kit allow、instance template digest
欠落を確認した。readiness reportは6 blockerを理由に`ready=false` / `safe_to_start_worker=false`のままで、
production attestationや実Codex/OpenCode連携の完了を意味しない。Oracle、coding agent、外部modelは
呼び出していない。

2026-08-31 06:09:46Z時点で、Humanのaction-time confirmation後にfresh
`sbx secret set openai --oauth` flowを完了した。CLIはtoken managerの更新とglobal scopeへの保存を報告し、
値を読み出さない`sbx secret ls`確認は`openai (oauth configured)`を返した。Host Codexも
`codex login status`でChatGPT loginを報告した。このcredential準備はproduction attestation、profile有効化、
live-agent smokeの代替ではなく、6 blockerと`safe_to_start_worker=false`は維持する。

2026-08-31 08:27:02Z時点で、最初のreal lifecycle observationは実schema、instance UUID、image digestを
取得する探索としては有効だったが、automatic ownership/cleanupの安全性を証明しなかった。後続auditで、
name-selected cleanupのTOCTOU、nonzero createと競合additionの因果不足、sandbox startupのshared
skills/MCP/credential/policy scopeが判明したため、そのmutation実装を完成扱いせず撤去した。historical raw
archiveと当時の結果は書き換えず、後続decisionとcontradictionとして残す。

代替として、実行可能なcontrol-plane経路をread-only `version`、`ls`、optional `inspect`へ限定した。
`status`はruntime validationで`observed`/`incomplete`以外を拒否し、`ready`、`safe_to_start_worker`、
`attestation_issued`は常にfalseである。version、inventory、optional name-selected inspectはsource
operation IDsへ明示的に結ぶが、UUID/workspace/imageのcomposite identityは生成せず、
`atomic_instance_binding_proven=false`を固定する。raw argv/stdout/stderrはdirfd/O_NOFOLLOW、0600、
fsync、atomic no-replace commitを通す。

実Host probe `obs_f58ae6caf1564d198feb8d8c7cfed1b6`は`sbx v0.39.0`と空inventoryを観測し、
manifest SHA-256 `b46838075c9851fa6744f221e921f6e1140680501daf29b5970bce2b0eee3049`で完了した。
side-effecting commandは発行せず、sandbox、Codex/OpenCode、OracleProvider、外部modelを起動していない。
productionの6 blockerとlive-agent skipは全て維持され、当初のoperational integration最終目的は未達である。

2026-08-31 09:58:14Zのarchive境界refactorでは、外部schema、filename、reason ID、raw bytes、hash、
provenance、non-authorizing flagsを変えずにsemantic validationとmanifest assemblyを一つのpayload層へ
集約した。canonical fixtureのmanifest SHA-256
`3f7a5067ccaa7e510bcbec1d001a68c1e380abd6f7aa1f9f13cdcb3a6d3df2ff`はrefactor前後で一致し、
whitespace、CRLF、NUL、不正UTF-8、malformed markupを含むstdout/stderrもbyte-for-byteで一致した。
arbitrary Protocol、forged real、seal/provenance/argv tamperはarchive root作成前に拒否する。

`wc -l`による対象production LOCは、refactor前の
`sbx_observation.py` 465 + `sbx_observation_archive.py` 851 + `sbx_probe.py` 752 = 2068行から、
refactor後の465 + `sbx_observation_payload.py` 658 + 406 + 453 = 1982行へ86行減少した。
format圧縮、型削除、filesystem check削除による削減ではない。production binding、6 blocker、
live-agent skipは変更せず、live `sbx`、sandbox、Codex/OpenCode、OracleProvider、外部modelは起動していない。

2026-08-31 11:12:53Zのfinal hardening後は、上記1982行の中間snapshotへimmutable copy、closure-sealed
real issuance、cross-field canonical validation、callback error normalization、artifact owner検査を追加した。
最終`wc -l`は`sbx_observation.py` 465 + `sbx_observation_payload.py` 738 +
`sbx_observation_archive.py` 410 + `sbx_probe.py` 453 = 2066行で、baseline 2068行から2行減少した。
圧縮記法、型・コメント・検査の削除は使っていない。

canonical fixtureのmanifest SHA-256は引き続き
`3f7a5067ccaa7e510bcbec1d001a68c1e380abd6f7aa1f9f13cdcb3a6d3df2ff`であり、raw bytes、filename、
hash、byte count、schema、reason ID、truth domain、actor/provenance、non-authorizing flagsは不変である。
focused suiteは131 passed、全体pytestは713 passed、1 live opt-in skip、`ruff check .`は成功した。
残した重複は、同じvalidatorをreport生成時とarchive snapshot生成時に呼ぶ二つの防御層と、archive固有の
dirfd/O_NOFOLLOW/fsync/no-replace処理だけである。`worker_archive.py`と`validation_archive.py`は変更していない。
live `sbx`、sandbox、Codex/OpenCode、OracleProvider、外部modelは起動していない。

2026-08-31 11:34:27Zの追加auditでは、public reason allowlist、exact provenance element type、
child dirfd close、mode/owner helperを単一正本へ固定した。artifact provenanceはvalidated snapshotの
timestamp/truth domainを直接使い、manifest組立て中の重複public変換を除去した。最終gate前のfocused suiteは
137 passedで、`wc -l`は`sbx_observation.py` 465 + `sbx_observation_payload.py` 756 +
`sbx_observation_archive.py` 392 + `sbx_probe.py` 453 = 2066行、baselineから2行減少した。
旧real archiveは書き換えず、current-schema replayをhistorical importとして扱わない。live `sbx`、sandbox、
Codex/OpenCode、OracleProvider、外部modelは起動していない。

2026-08-31 11:41:49Zの最終同一snapshotでは、指定focused suiteが`137 passed in 0.90s`、
全体pytestが`719 passed, 1 skipped in 37.01s`だった。skipは
`ORACLE_LAB_RUN_LIVE_AGENT_TESTS=1`を要求する明示live-agent opt-inだけである。
`ruff format --check .`、`ruff check .`、ExecPlan validator、`git diff --check`も成功し、
二系統の独立read-only auditは残存P0/P1なしと判定した。

2026-08-31 11:46:51Zに最後の重複type gateをpayload builderへ一本化した後も、canonical manifest hash、
arbitrary Protocol非実行、forged real/provenance/argv拒否のfocused 137 testsは全件成功した。
最終production LOCは465 + 756 + 389 + 453 = 2063行で、baseline 2068行から5行減少した。

## Context and Orientation

対象リポジトリは `/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab` である。
この計画におけるOracleは観察対象のモデル、Hostは保存、分析、実行、routingを担当する
制御層、WorkerはCodexまたはOpenCodeのようなコーディングエージェント、Humanはpatchを
採用または拒否する利用者を意味する。

`candidate patch` はエージェントが提案した未採用のコード差分である。これは成果物では
あるが、承認前はリポジトリ状態ではない。`standalone staging clone` は承認済みpatchを安全に
適用して検証するOracle Lab管理下のsource-independent Git作業領域であり、利用者が現在開いている
worktreeとは別物で、sourceとのremote、alternates、共有objectを持たない。`write-once archive` は同じidentityの内容を上書きしない保存領域を
意味する。`truth domain` はツール結果がどの現実領域から得られたかを表し、この計画で
sandbox内のテスト結果には `sandbox` を付ける。

現在のアダプター実装は
`/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab/src/oracle_lab/agent_adapters.py`
にある。`WorkerTask` はsource event、関連claim、直近20件のイベント、goalから
エージェント向け入力を構築する。`OpenCodeAdapter` と `CodexAdapter` は外部CLIを起動し、
stdoutから構造化された `analysis.*` イベントだけを抽出する。`WorkerRouter` は軽量な
Direct APIタスクとコーディングエージェント向けタスクを分類する。

サービスとの接続は
`/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab/src/oracle_lab/services.py`
の `host_worker_router`、`_execute_host_worker_job`、`run_automation` にある。標準CLIの
service factoryは同ファイルの `OracleLabService.default()` を使用し、`config/agents.toml`から
disabled-by-defaultのrouterとbrokerを構築する。Direct Hostは標準接続できるが、coding profileは
不足しているproduction isolation evidenceが揃うまでbroker bind時にfail closedする。

イベント型は
`/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab/src/oracle_lab/events.py`、
append時の構造的な強制境界は
`/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab/src/oracle_lab/store.py`、
CLIは `/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab/src/oracle_lab/cli.py` にある。
repository edit機能はこれらすべてにまたがるが、OracleProviderとoracle output経路を
変更してはならない。

## Plan of Work

最初のマイルストーンでは、`config/agents.toml` と設定モデルを追加する。標準状態は
`enabled=false` とする。Codex、OpenCode、Direct APIを個別に有効化できるようにし、
優先順位とフォールバックは明示的に設定された組み合わせの中だけで行う。
`OracleLabService.default()` はこの設定を読み、enabledなworkerだけから
`WorkerRouter` を構築する。軽量タスクに対応するDirect APIがない場合はfail closedとし、
高価で権限の強いcoding agentへ暗黙的に切り替えない。

第2マイルストーンでは、worker taskのexact prompt、CLI引数、adapter identity、
実行ファイルversion、base commit、stdout、stderrを保存するwrite-once archiveを追加する。
model名やCLI versionを取得できない場合は推測せず、明示的なunknownとして保存する。
イベントにはraw stdout/stderrを複製せず、archive path、SHA-256、byte countを記録する。

第3マイルストーンでは、repository editを隔離されたrepository workspaceで実行し、
実行後に `git diff --binary --no-ext-diff` 相当のpatchを回収する。patchのSHA-256と
base commitを保存し、trusted Hostがpath、file mode、symlink、submodule、base commit、
patch hash、target preconditionを検査するsecurity preflightを実行する。合格したときだけ
`worker.patch_proposed` をappendする。元のrepositoryまたは
現在の作業ツリーに変更が発生した場合はrunを失敗として扱う。patchに絶対パス、
repository外へのpath traversal、未許可symlink、submodule変更、`.git` 内部変更が含まれる
場合は取り込みを拒否し、Human gateやstandalone staging cloneを作成しない。

第4マイルストーンでは、candidate patchに対する人間の承認と拒否を実装する。
承認前にはpatchを適用しない。承認後は、記録されたbase commitから専用standalone staging cloneを
作成し、patch hashを再検証して適用する。base commitまたは対象ファイルのpreconditionが
変化している場合は自動解決せず、conflictをイベントとして記録して停止する。

第5マイルストーンでは、standalone staging clone上で設定されたlint、test、type-check commandを
sandbox経由で実行する。結果はcommand、exit code、stdout/stderr archive、truth domain、
patch event ID、approval event ID、application event IDとともに保存する。検証失敗はpatchの
自動修正、自動commit、push、merge、または採用を意味しない。

最後のマイルストーンでは、worker操作用CLI、fake agentによる決定論的E2E試験、
operator opt-inによる実Codex/OpenCode smoke test、READMEを追加する。実agent smoke testは
既定のテストスイートから外し、外部呼び出し、費用、認証が明示されたときだけ実行する。
OSレベルのworker隔離が実装されるまではlive testはopt-in時もskipし、Host上で
未隔離のagent subprocessを起動しない。

## Concrete Steps

作業ディレクトリは常に
`/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab` とする。主要な変更対象は次のとおり。

    config/agents.toml
    src/oracle_lab/config.py
    src/oracle_lab/events.py
    src/oracle_lab/agent_adapters.py
    src/oracle_lab/worker_readiness.py
    src/oracle_lab/git_control.py
    src/oracle_lab/host_provider.py
    src/oracle_lab/jobs.py
    src/oracle_lab/services.py
    src/oracle_lab/cli.py
    src/oracle_lab/store.py
    src/oracle_lab/projections.py
    src/oracle_lab/worker_archive.py
    src/oracle_lab/validation_archive.py
    src/oracle_lab/bundle_import.py
    src/oracle_lab/patches.py
    tests/test_agent_adapter_runtime.py
    tests/test_agent_adapter_service_integration.py
    tests/test_agent_adapter_repository_edit.py
    tests/test_candidate_patches.py
    tests/test_coding_isolation_contract.py
    tests/test_docker_sbx_isolation.py
    tests/test_service_isolation_wiring.py
    tests/test_worker_readiness.py
    tests/test_live_agent_opt_in.py

各マイルストーンの実装前に `Progress` を更新し、focused testsを実行する。

    cd /Users/annenpolka/ghq/github.com/annenpolka/oracle_lab
    env UV_CACHE_DIR=/private/tmp/oracle-lab-uv-cache \
      uv run pytest \
      tests/test_agent_adapter_runtime.py \
      tests/test_agent_adapter_service_integration.py \
      tests/test_agent_adapter_repository_edit.py \
      tests/test_candidate_patches.py \
      tests/test_coding_isolation_contract.py \
      tests/test_docker_sbx_isolation.py \
      tests/test_service_isolation_wiring.py \
      tests/test_worker_readiness.py \
      tests/test_live_agent_opt_in.py -q

実装途中では、まだ作成されていないテストファイルをcommandから一時的に外してよいが、
停止時に `Progress` へ残作業を明記する。focused testsの期待結果は全件passであり、skipは
実agent用の明示的なlive testに限る。

実装後のCLIは次の操作を提供する。

    uv run oracle worker enqueue repository-edit \
      --source evt_... --goal "Implement the cited change only."
    uv run oracle run --until-human
    uv run oracle worker patch show evt_patch_...
    uv run oracle worker patch approve evt_patch_...
    uv run oracle run --until-human
    uv run oracle worker patch status evt_patch_...

`patch show` はbase commit、変更対象、patch SHA-256、worker identity、source event IDsを表示する。
`patch approve` はHuman actorの承認イベントをappendしてapply jobをenqueueするだけとし、
CLI process内で直接patchを適用しない。最後の `patch status` はstaging path、適用状態、
validation event IDsを表示する。

最終確認では次を実行し、すべてexit code 0を期待する。

    env UV_CACHE_DIR=/private/tmp/oracle-lab-uv-cache uv run ruff format --check .
    env UV_CACHE_DIR=/private/tmp/oracle-lab-uv-cache uv run ruff check .
    env UV_CACHE_DIR=/private/tmp/oracle-lab-uv-cache uv run pytest -q

実agent smoke testはoperatorが外部呼び出しを明示的に許可した場合だけ実行する。

    ORACLE_LAB_RUN_LIVE_AGENT_TESTS=1 \
      ORACLE_LAB_LIVE_AGENT=codex \
      ORACLE_LAB_LIVE_SBX_TEMPLATE='registry.example/codex@sha256:<64-lowercase-hex>' \
      ORACLE_LAB_LIVE_ALLOWED_HOSTS='api.example.invalid,auth.example.invalid' \
      env UV_CACHE_DIR=/private/tmp/oracle-lab-uv-cache \
      uv run pytest -m live_agent tests/test_live_agent_opt_in.py -q

現在の `tests/test_live_agent_opt_in.py` は安全な多段gateである。opt-in、digest固定template、
exact host listが揃うまではsubprocess前にskipする。すべて揃った場合もproduction conformanceが
成功しなければcoding agentを起動しない。現状のproduction brokerは不足証拠を理由にfail closedする。

## Validation and Acceptance

fake coding agentがworkspace内のファイルを変更した場合、その変更がcandidate patchとして
保存され、元の作業ツリーのHEAD、index、dirty file content、untracked file contentを含む
fingerprintが実行前後で一致すれば合格とする。`git status --short` の文字列一致だけでは
既にdirtyなファイルへの追加変更を検出できないため、受け入れ証拠としては不十分である。
新しいテスト `test_repository_edit_archives_patch_without_touching_source_worktree` は変更前に失敗し、
実装後にpassしなければならない。

security preflightがunsafe path、symlink、submodule、agent commit、base/hash/precondition不一致を
発見した場合、`worker.patch_proposed` もHuman approval jobも生成されなければ合格とする。

candidate patchに人間の承認が存在しない状態でapply処理を呼び出した場合、処理が拒否され、
standalone staging cloneが作成されなければ合格とする。新しいテスト
`test_patch_application_requires_matching_human_approval` で証明する。

一致するHuman actorの承認イベントが存在する場合にだけstandalone staging cloneへpatchが適用され、
適用イベントが承認イベントとpatch eventの両方を引用すれば合格とする。base commit、
patch hash、対象ファイルpreconditionのいずれかが一致しない場合は、部分適用せず
`worker.validation_failed` または専用conflict eventを記録しなければならない。

エージェントが `oracle.output`、`claim.promoted`、`human.keep`、`human.star`、canon promotion、
world-state変更イベントを返した場合、ingest境界で拒否されれば合格とする。candidate patchは
oracle corpus、claim history、motif statistics、curation viewへ入ってはならない。

validation commandの結果が `truth_domain=sandbox` を持ち、patch event、command、
stdout/stderr archiveへ遡れれば合格とする。workerが自分で記述した「tests passed」という文章を
検証結果として採用してはならず、決定論的なsandbox runnerのexit codeを使用する。

同じidempotency keyのworker taskを再実行しても、二重patch、二重承認、二重適用が発生しなければ
合格とする。timeout、output limit超過、CLI異常終了時に部分patchを採用せず、workspaceを清掃し、
失敗理由をイベントとして残さなければならない。

実Codex/OpenCode smoke testでは、明示的なoperator opt-inがない限りsubprocess起動より
前にskipし、外部モデルを呼び出さない。実起動を有効化する将来のテストは小さな
fixture repositoryだけを渡し、現在のOracle Lab repositoryを変更しない。

brokerがtimeout、output limit、nonzero worker exit、workspace quiescence不明、guest Git control
不明、sandbox cleanup不明のいずれかを観測した場合、workspace exportをcandidateとして取り込まず、
Human gateも作成しなければ合格とする。synthetic fixtureのpassing receiptはproduction routerを
有効化できず、実tool domainの観測としてarchiveされてはならない。

read-only `sbx` observationは明示gateなしにsubprocessを起動せず、gateありでもargvの第2要素が
`version`、`ls`、`inspect`以外ならテストで失敗しなければならない。任意のinjected runnerが
`production`を自己申告してもreal truth domainを発行できず、requested/returned argv不一致、
schema drift、name-selected view不整合は`incomplete`としてfail closedする。reportは`passed`を受理せず、
`IsolationAttestation`をimportまたは生成しない。

observation archiveはexact argv/stdout/stderrを0600で保持し、public JSONとmanifest本体へraw値を
埋め込まず、version/inventory/name-selected inspectをsource operation IDsへ結ぶ。symlink ancestor、existing final、
commit race、unsafe permissions、atomic no-replace非対応ではcanonical archiveを生成しなければ合格とする。

## Idempotence and Recovery

worker task、candidate patch、approval、application、validationにはそれぞれ安定した
idempotency keyを持たせる。同一taskを再leaseした場合、完了済みrunまたは既存patchを返し、
agentを再起動しない。明示的なretryは新しいrun IDを作るが、元runとのcausationを保存する。

archiveの作成後にDB appendが失敗した場合、同じhashの孤立archiveを再利用できるようにする。
内容が異なる同名archiveは上書きしない。archiveの一部だけが存在する場合はincomplete状態として
検出し、正しいhashの再構築または明示的なquarantine以外で利用しない。

patch適用中に失敗した場合はstandalone staging cloneを破棄し、同じbase commitから再作成する。
現在の作業ツリーをrollback対象にしてはならない。適用後のvalidation失敗ではstaging cloneを
保持して調査可能にするが、成功扱いまたは自動mergeは行わない。

providerまたはagent CLIが失敗した場合、自動再試行は設定された回数とautomation budget内に
限定する。繰り返し同一失敗を検出した場合は停止し、人間へ戻す。明示的なpause eventがある
セッションでは新しいworker processを起動しない。

## Artifacts and Notes

各worker runについて、次の成果物をwrite-onceで保存する。

    archive/workers/YYYY/MM/DD/<run-id>/
    ├── task.json
    ├── prompt.txt
    ├── command.json
    ├── stdout.bin
    ├── stderr.bin
    ├── patch.diff
    └── metadata.json

`metadata.json` には各ファイルのSHA-256、adapter種別、実行ファイルversion、base commit、
開始・終了時刻、exit code、timeout、output limit、model identityの既知・未知状態を保存する。
秘密値は保存せず、渡した環境変数名とredaction状態だけを記録する。

実装中に重要なテスト結果、live smoke testのCLI version、既知のsandbox制約をこのセクションへ
追記する。stdoutやdiff全体はここへ複製せず、archive pathとhashだけを記録する。

2026-08-30 20:59:03Zの文書・opt-in gate検証では、ExecPlan validatorは成功し、
`tests/test_live_agent_opt_in.py` は通常実行で1 passed、1 skipped、明示opt-in実行で
1 skippedとなり、どちらもagent subprocessを起動しなかった。対象ファイルの
Ruff format/checkは成功した。並行実装取り込み後の全体gateは上記のmigration試験と
repository-edit統合試験の更新を必要とする。

2026-08-31のpath/index回帰では、`tests/test_agent_adapter_repository_edit.py` と
`tests/test_agent_adapter_service_integration.py` が18 passedとなった。default cwdのfull
enqueue/run/archive/approve/apply後もsourceのHEAD、status、raw index、stage entries/flags、
非`.git` tree fingerprintが一致した。全体gateは最終統合後に再実行する。

2026-08-30 23:13:28Zの最終統合スナップショットでは、ExecPlan validatorが成功した。
`ruff format --check .`は`105 files already formatted`、`ruff check .`は`All checks passed!`、
compileallは成功し、全体pytestは`473 passed, 1 skipped in 32.02s`だった。skipは
`tests/test_live_agent_opt_in.py`のoperator opt-in前gateだけで、外部agent subprocessは起動して
いない。既報のOracle ancestry、lease owner、atomic enqueue、immutable validation tree、single
terminal、canon binding、Direct Hostのoutput/fallback/credential境界を再確認する選抜39件も
全件passした。

2026-08-31 04:14:06Zのisolation hardeningスナップショットでは、次を追加した。

    src/oracle_lab/coding_isolation.py
    src/oracle_lab/docker_sbx_isolation.py
    src/oracle_lab/workspace_archive.py
    tests/test_coding_isolation_contract.py
    tests/test_docker_sbx_isolation.py
    tests/test_workspace_archive.py
    tests/test_workspace_archive_adapter_integration.py
    tests/test_service_isolation_wiring.py
    tests/test_coding_isolation_archive.py

ExecPlan validatorは成功し、`ruff format --check .`は`115 files already formatted`、
`ruff check .`は`All checks passed!`、compileallは成功、全体pytestは
`633 passed, 1 skipped in 33.25s`だった。skipはoperator opt-in前のlive-agent gateだけである。
synthetic lifecycle testはbackend、receipt、全capability evidenceを`synthetic_fixture`として保持し、
production `SubprocessCommandRunner.bind()`は`sbx` subprocess前にfail closedした。Docker sandbox、
Codex/OpenCode、OracleProvider、外部modelはいずれも起動していない。

2026-08-31 05:25:22Zのoperational-readiness作業では次を追加した。

    src/oracle_lab/worker_readiness.py
    tests/test_worker_readiness.py

`oracle worker readiness`は`config/agents.toml`とbroker executable bytesだけを読み、service、DB、archive、
subprocess、sandbox、modelを起動しない。実Host観測（`truth_domain=real`）ではstandalone
`sbx v0.39.0`、Apple virtualization、daemon、Docker service OAuth、storage、version一致を確認し、
`sbx diagnose -o json`は12 passed、0 failed、0 skippedだった。global network policyは`deny-all`である。

一時fixture repositoryを使ったguest観測（`truth_domain=sandbox`）では、source mount write不可、private
cloneへのfile/Git config変更、`example.com`への403 default deny、kit-scoped `openrouter.ai`への200、
cleanup後の`sbx ls --json`空配列を確認した。sandbox名は`oracle-lab-preflight-xsrxk3`、instance UUIDは
`2d996d24-d758-455d-8d08-36d082372f82`だった。sandboxとfixture directoryは削除し、sandboxdも停止した。
このmanual development probeはOracle Labのproduction conformance receiptまたはworker archiveではなく、
実model、Codex/OpenCode、OracleProviderを呼んでいない。

Codex用の`sbx secret set openai --oauth`は、永続的なOAuth許可の同意画面でHumanのaction-time
confirmationを待つ間に2回timeoutした。`sbx secret ls`に保存済みsecretはなく、tokenは保存されていない。
次回はHuman確認後にfresh OAuth flowを開始する。Docker serviceへのログイン成功およびdiagnose成功は、
この未完了credentialやOracle Lab production attestationを代替しない。

2026-08-31 06:09:46Zのfresh OAuth flowではcallbackが成功し、CLIがglobal scopeへのtoken保存を報告した。
値を読み出さずに`sbx secret ls`を実行し、`openai (oauth configured)`だけを確認した。前段落のtimeoutは
履歴として保持し、credential準備の完了をproduction conformance receiptとは扱わない。

readiness統合後の同一スナップショットでは、focused readiness/CLI/isolation/live-gate suiteが
`95 passed, 1 skipped`、コミット前に再実行した全体pytestが`644 passed, 1 skipped in 46.86s`だった。`ruff format --check .`は
`117 files already formatted`、`ruff check .`、compileall、ExecPlan validator、`git diff --check`は成功した。
skipはoperator opt-in前のlive-agent gateだけである。

2026-08-31 08:32:13Zのread-only observation hardeningでは次を追加した。

    src/oracle_lab/sbx_observation.py
    src/oracle_lab/sbx_observation_archive.py
    src/oracle_lab/sbx_observation_payload.py
    src/oracle_lab/sbx_probe.py
    tests/test_sbx_observation.py
    tests/test_sbx_observation_archive.py
    tests/test_sbx_probe.py

`oracle worker isolation probe --observe-read-only-control-plane`はservice/EventStoreを生成せず、
fixed read-only argvだけを実行する。実probe `obs_f58ae6caf1564d198feb8d8c7cfed1b6`のarchiveは
`/Users/annenpolka/.oracle_lab/sbx-observations/`にあり、manifest SHA-256は
`b46838075c9851fa6744f221e921f6e1140680501daf29b5970bce2b0eee3049`、raw artifactは6件で全て
0600、inventoryは空だった。focused security suiteは`240 passed, 1 skipped in 2.47s`、全体pytestは
`683 passed, 1 skipped in 38.12s`だった。`ruff format --check .`は`123 files already formatted`、
`ruff check .`、compileall、ExecPlan validator、`git diff --check`は成功した。

未完了のartifactはproduction conformance receiptとlive smoke archiveである。現行の不足証拠を
fixtureやHost推論で補わず、workspace quiescence、guest Git control、data-plane network/credential、
actual template instance identity、sandbox ownership、profile/workspace bindingを実測できた後にのみ、
このセクションへ実run ID、CLI version、archive hashを追記する。

## Interfaces and Dependencies

`/Users/annenpolka/ghq/github.com/annenpolka/oracle_lab/config/agents.toml` にworker設定を追加する。
設定は少なくともenabled、adapter、executable、model、timeout_seconds、max_output_bytes、
sandbox_profile、allowed_environment_names、fallback_adapterを持つ。資格情報の値は設定snapshotへ
保存しない。

`src/oracle_lab/agent_adapters.py` に `WorkerExecutionProfile` を追加する。これはadapter種別、
実行ファイル、model指定、timeout、出力上限、sandbox policy、許可された環境変数名を保持する
immutable dataclassとする。

`src/oracle_lab/worker_archive.py` に `WorkerRunArchive` と `WorkerArchiveRecord` を追加する。
`WorkerRunArchive.write(...)` はrun ID、raw artifacts、metadataを受け取り、O_EXCL相当の
write-once操作で保存し、各pathとSHA-256を返す。partial write時のcleanupと同一hash orphanの
安全な再利用をテスト可能な関数へ分離する。

`src/oracle_lab/patches.py` にpureな `CandidatePatch`、preflight、application helperを置く。
`CandidatePatch` はworker run ID、source event IDs、
base commit、patch SHA-256、変更対象path、artifact originを持つ。artifact originは
`worker_generated` とし、oracle material originとは別のnamespaceで保持する。

`OracleLabService.approve_patch()` と `reject_patch()` はHuman actorのappend-only judgmentと
durable jobを同一transactionで生成する。`_execute_patch_application_job()` は承認、base commit、
hash、path policyを再検証し、専用standalone staging cloneへ適用する。worker出力からこの経路を
直接呼び出してはならない。

`src/oracle_lab/events.py` へ少なくとも次のイベント型を追加する。

    worker.task_requested
    worker.run_started
    worker.run_completed
    worker.run_failed
    worker.patch_proposed
    human.patch_approved
    human.patch_rejected
    worker.patch_applied
    worker.validation_completed
    worker.validation_failed

`worker.patch_proposed` はsource event IDs、worker run ID、base commit、patch archive path、
patch SHA-256を必須とする。`human.patch_approved` と `human.patch_rejected` はHuman actor以外からの
生成をstore境界で拒否する。`worker.patch_applied` はagent自身ではなく、検証済みの決定論的な
application serviceが生成する。validation resultは `truth_domain=sandbox` とpatch event IDを
必須とする。

`src/oracle_lab/cli.py` に `worker` command groupを追加し、enqueue、run状態表示、patch show、
approve、reject、statusを提供する。CLIは人間操作をHuman actorとして記録するが、agent run、
patch apply、validationはdurable jobとして実行する。

`repository-edit` jobはworker-generated artifactだけを生成し、modelが返した `analysis.*` を
Oracle研究projectionへingestしない。Oracleへの入力が必要な操作は別の
`prompt-oracle` capabilityとして、exact prompt、configured `OracleProvider`、model identity、
raw response archiveの既存契約を通す。coding agentはOracle出力の代替を生成しない。

新しい外部Python依存関係は原則追加しない。Git操作は引数配列を使ったnon-interactive
subprocessとして実行し、shell文字列を組み立てない。sandbox実装は既存のtool sandbox境界を
再利用できる場合は再利用するが、coding agent control planeとagentが生成したcommandの
execution planeを混同してはならない。
