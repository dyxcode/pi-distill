import { ConsolidationCommits } from './commits.ts';
import { StorageConnection } from './connection.ts';
import { SourceCoverage } from './coverage.ts';
import { DistillQueue } from './distill.ts';
import { MetadataMaintenance } from './maintenance.ts';
import { Memories } from './memories.ts';
import { RetentionTasks } from './retention.ts';
import { SchemaManager } from './schema.ts';
import { SessionRecords } from './sessions.ts';
import { SourceIndex } from './source-index.ts';
import { SourceRegistry } from './source-registry.ts';
import { SourceViews } from './source-view.ts';
export { ConsolidationConflictError } from '../distill/types.ts';
export { MemoryConflictError, MemoryNotFoundError } from '../memory/types.ts';
export type { Memory, MemoryForget, MemoryInput, MemoryOrigin, MemorySearchRequest, MemorySearchRow, MemoryUpdate } from '../memory/types.ts';
export type { SessionDeletion } from '../retention/types.ts';
export { processHasExited } from './session-validation.ts';

/** Application storage boundary. All operations share one connection and atomic commit owner. */
export class SQLiteMemoryStore {
  readonly #connection: StorageConnection;
  #sources: SourceRegistry | undefined;
  get sources(): SourceRegistry { return this.#sources ??= new SourceRegistry(this.#connection.database); }
  #sourceIndex: SourceIndex | undefined;
  get sourceIndex(): SourceIndex { return this.#sourceIndex ??= new SourceIndex(this.#connection.database); }
  #sourceViews: SourceViews | undefined;
  get sourceViews(): SourceViews { return this.#sourceViews ??= new SourceViews(this.#connection.database, this.sourceIndex); }
  get sourceCoverage(): SourceCoverage { this.#connection.ensureOpen(); return new SourceCoverage(this.#connection.database, this); }
  readonly save: Memories['save'];
  readonly update: Memories['update'];
  readonly forget: Memories['forget'];
  readonly get: Memories['get'];
  readonly memoryOrigin: ConsolidationCommits['memoryOrigin'];
  readonly searchBatch: Memories['searchBatch'];
  readonly enqueueDistillTrigger: DistillQueue['enqueueDistillTrigger'];
  readonly confirmCompactionTrigger: DistillQueue['confirmCompactionTrigger'];
  readonly cancelCompactionTrigger: DistillQueue['cancelCompactionTrigger'];
  readonly listDistillTriggers: DistillQueue['listDistillTriggers'];
  readonly readDistillTrigger: DistillQueue['readDistillTrigger'];
  readonly readDistillFailure: DistillQueue['readDistillFailure'];
  readonly canAttemptDistill: DistillQueue['canAttemptDistill'];
  readonly listScheduledDistill: DistillQueue['listScheduledDistill'];
  readonly readyDistillWork: DistillQueue['readyDistillWork'];
  readonly distillWakeDelay: DistillQueue['distillWakeDelay'];
  readonly claimScheduledDistill: DistillQueue['claimScheduledDistill'];
  readonly settleScheduledDistill: DistillQueue['settleScheduledDistill'];
  readonly claimDistillTrigger: DistillQueue['claimDistillTrigger'];
  readonly readSourcePreparation: DistillQueue['readSourcePreparation'];
  readonly pendingTriggerJob: DistillQueue['pendingTriggerJob'];
  readonly pendingJobForSource: DistillQueue['pendingJobForSource'];
  readonly prepareDistillPage: DistillQueue['prepareDistillPage'];
  readonly prepareDistillTrigger: DistillQueue['prepareDistillTrigger'];
  readonly claimDistillJob: DistillQueue['claimDistillJob'];
  readonly renewDistillClaim: DistillQueue['renewDistillClaim'];
  readonly releaseDistillClaim: DistillQueue['releaseDistillClaim'];
  readonly enqueueCleanTask: RetentionTasks['enqueueCleanTask'];
  readonly maintenanceBoundary: RetentionTasks['maintenanceBoundary'];
  readonly listCleanTasks: RetentionTasks['listCleanTasks'];
  readonly claimCleanTask: RetentionTasks['claimCleanTask'];
  readonly releaseCleanTask: RetentionTasks['releaseCleanTask'];
  readonly recordCleanAssessment: RetentionTasks['recordCleanAssessment'];
  readonly reconcileLegacyCleanTasks: RetentionTasks['reconcileLegacyCleanTasks'];
  readonly enqueueConsolidation: DistillQueue['enqueueConsolidation'];
  readonly listConsolidationJobs: DistillQueue['listConsolidationJobs'];
  readonly listPendingConsolidationJobs: DistillQueue['listPendingConsolidationJobs'];
  readonly claimConsolidation: DistillQueue['claimConsolidation'];
  readonly releaseConsolidation: DistillQueue['releaseConsolidation'];
  readonly processedSources: ConsolidationCommits['processedSources'];
  readonly readConsolidationStatus: ConsolidationCommits['readConsolidationStatus'];
  readonly commitConsolidation: ConsolidationCommits['commitConsolidation'];
  readonly observeSession: SessionRecords['observeSession'];
  readonly saveSessionUsage: SessionRecords['saveSessionUsage'];
  readonly acknowledgeObservation: SessionRecords['acknowledgeObservation'];
  readonly observedSessions: SessionRecords['observedSessions'];
  readonly listObservedSessions: SessionRecords['listObservedSessions'];
  readonly listUnobservedSessionUsages: SessionRecords['listUnobservedSessionUsages'];
  readonly listSessionUsages: SessionRecords['listSessionUsages'];
  readonly listKnownSessionFiles: SessionRecords['listKnownSessionFiles'];
  readonly reserveSessionSource: SessionRecords['reserveSessionSource'];
  readonly backfillSources: MetadataMaintenance['backfillSources'];
  readonly collectMetadata: MetadataMaintenance['collectMetadata'];
  readonly hasUncapturedObservation: RetentionTasks['hasUncapturedObservation'];
  readonly hasSessionSourceWork: RetentionTasks['hasSessionSourceWork'];
  readonly acquireSourceAccess: RetentionTasks['acquireSourceAccess'];
  readonly releaseSourceAccess: RetentionTasks['releaseSourceAccess'];
  readonly sourceRetentionState: RetentionTasks['sourceRetentionState'];
  readonly retentionVersion: RetentionTasks['retentionVersion'];
  readonly reconcileExitedSessionOwners: RetentionTasks['reconcileExitedSessionOwners'];
  readonly getDeletion: RetentionTasks['getDeletion'];
  readonly prepareDeletion: RetentionTasks['prepareDeletion'];
  readonly finishDeletion: RetentionTasks['finishDeletion'];

  constructor(path: string) {
    const connection = this.#connection = new StorageConnection(path);
    try {
      const store = this;
      const memories = new Memories(connection);
      const commits = new ConsolidationCommits(connection, {
        get retention(): RetentionTasks { return retention; },
        get distill(): DistillQueue { return distill; },
        get memories(): Memories { return memories; },
        get sessions(): SessionRecords { return sessions; },
        get sources(): SourceRegistry { return store.sources; }
      });
      const distill = new DistillQueue(connection, {
        get sessions(): SessionRecords { return sessions; },
        get retention(): RetentionTasks { return retention; }
      });
      const sessions = new SessionRecords(connection, {
        get sources(): SourceRegistry { return store.sources; },
        get distill(): DistillQueue { return distill; },
        get retention(): RetentionTasks { return retention; }
      });
      const retention = new RetentionTasks(connection, {
        get distill(): DistillQueue { return distill; },
        get sources(): SourceRegistry { return store.sources; },
        get sessions(): SessionRecords { return sessions; }
      });
      const maintenance = new MetadataMaintenance(connection, {
        get sourceViews(): SourceViews { return store.sourceViews; },
        get sourceIndex(): SourceIndex { return store.sourceIndex; },
        get retention(): RetentionTasks { return retention; },
        get sources(): SourceRegistry { return store.sources; },
        get distill(): DistillQueue { return distill; }
      });
      const schema = new SchemaManager(connection, {
        get retention(): RetentionTasks { return retention; },
        get maintenance(): MetadataMaintenance { return maintenance; },
        get commits(): ConsolidationCommits { return commits; },
        get sources(): SourceRegistry { return store.sources; },
        get distill(): DistillQueue { return distill; }
      });
      connection.transaction(() => schema.initializeSchema());
      this.save = memories.save.bind(memories);
      this.update = memories.update.bind(memories);
      this.forget = memories.forget.bind(memories);
      this.get = memories.get.bind(memories);
      this.memoryOrigin = commits.memoryOrigin.bind(commits);
      this.searchBatch = memories.searchBatch.bind(memories);
      this.enqueueDistillTrigger = distill.enqueueDistillTrigger.bind(distill);
      this.confirmCompactionTrigger = distill.confirmCompactionTrigger.bind(distill);
      this.cancelCompactionTrigger = distill.cancelCompactionTrigger.bind(distill);
      this.listDistillTriggers = distill.listDistillTriggers.bind(distill);
      this.readDistillTrigger = distill.readDistillTrigger.bind(distill);
      this.readDistillFailure = distill.readDistillFailure.bind(distill);
      this.canAttemptDistill = distill.canAttemptDistill.bind(distill);
      this.listScheduledDistill = distill.listScheduledDistill.bind(distill);
      this.readyDistillWork = distill.readyDistillWork.bind(distill);
      this.distillWakeDelay = distill.distillWakeDelay.bind(distill);
      this.claimScheduledDistill = distill.claimScheduledDistill.bind(distill);
      this.settleScheduledDistill = distill.settleScheduledDistill.bind(distill);
      this.claimDistillTrigger = distill.claimDistillTrigger.bind(distill);
      this.readSourcePreparation = distill.readSourcePreparation.bind(distill);
      this.pendingTriggerJob = distill.pendingTriggerJob.bind(distill);
      this.pendingJobForSource = distill.pendingJobForSource.bind(distill);
      this.prepareDistillPage = distill.prepareDistillPage.bind(distill);
      this.prepareDistillTrigger = distill.prepareDistillTrigger.bind(distill);
      this.claimDistillJob = distill.claimDistillJob.bind(distill);
      this.renewDistillClaim = distill.renewDistillClaim.bind(distill);
      this.releaseDistillClaim = distill.releaseDistillClaim.bind(distill);
      this.enqueueCleanTask = retention.enqueueCleanTask.bind(retention);
      this.maintenanceBoundary = retention.maintenanceBoundary.bind(retention);
      this.listCleanTasks = retention.listCleanTasks.bind(retention);
      this.claimCleanTask = retention.claimCleanTask.bind(retention);
      this.releaseCleanTask = retention.releaseCleanTask.bind(retention);
      this.recordCleanAssessment = retention.recordCleanAssessment.bind(retention);
      this.reconcileLegacyCleanTasks = retention.reconcileLegacyCleanTasks.bind(retention);
      this.enqueueConsolidation = distill.enqueueConsolidation.bind(distill);
      this.listConsolidationJobs = distill.listConsolidationJobs.bind(distill);
      this.listPendingConsolidationJobs = distill.listPendingConsolidationJobs.bind(distill);
      this.claimConsolidation = distill.claimConsolidation.bind(distill);
      this.releaseConsolidation = distill.releaseConsolidation.bind(distill);
      this.processedSources = commits.processedSources.bind(commits);
      this.readConsolidationStatus = commits.readConsolidationStatus.bind(commits);
      this.commitConsolidation = commits.commitConsolidation.bind(commits);
      this.observeSession = sessions.observeSession.bind(sessions);
      this.saveSessionUsage = sessions.saveSessionUsage.bind(sessions);
      this.acknowledgeObservation = sessions.acknowledgeObservation.bind(sessions);
      this.observedSessions = sessions.observedSessions.bind(sessions);
      this.listObservedSessions = sessions.listObservedSessions.bind(sessions);
      this.listUnobservedSessionUsages = sessions.listUnobservedSessionUsages.bind(sessions);
      this.listSessionUsages = sessions.listSessionUsages.bind(sessions);
      this.listKnownSessionFiles = sessions.listKnownSessionFiles.bind(sessions);
      this.reserveSessionSource = sessions.reserveSessionSource.bind(sessions);
      this.backfillSources = maintenance.backfillSources.bind(maintenance);
      this.collectMetadata = maintenance.collectMetadata.bind(maintenance);
      this.hasUncapturedObservation = retention.hasUncapturedObservation.bind(retention);
      this.hasSessionSourceWork = retention.hasSessionSourceWork.bind(retention);
      this.acquireSourceAccess = retention.acquireSourceAccess.bind(retention);
      this.releaseSourceAccess = retention.releaseSourceAccess.bind(retention);
      this.sourceRetentionState = retention.sourceRetentionState.bind(retention);
      this.retentionVersion = retention.retentionVersion.bind(retention);
      this.reconcileExitedSessionOwners = retention.reconcileExitedSessionOwners.bind(retention);
      this.getDeletion = retention.getDeletion.bind(retention);
      this.prepareDeletion = retention.prepareDeletion.bind(retention);
      this.finishDeletion = retention.finishDeletion.bind(retention);
    } catch (error) { connection.close(); throw error; }
  }

  close(): void { this.#connection.close(); }
}
