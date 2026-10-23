import type { Api, AssistantMessage, Context, Model, ModelsApiStreamOptions } from '@earendil-works/pi-ai';

/** Capabilities consumed by background distillation, independent of the Pi UI. */
export interface ModelRuntime {
  readonly model: Model<Api> | undefined;
  readonly modelRegistry: {
    complete<TApi extends Api>(model: Model<TApi>, context: Context, options?: ModelsApiStreamOptions<TApi>): Promise<AssistantMessage>;
  };
}

export interface DistillRuntime extends ModelRuntime {
  readonly sessionManager: {
    getSessionFile(): string | undefined;
    getSessionId(): string;
    getCwd(): string;
    getLeafId(): string | null;
  };
  isIdle?(): boolean;
  hasPendingMessages?(): boolean;
}
