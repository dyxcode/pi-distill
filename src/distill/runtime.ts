import type { Api, AssistantMessage, AssistantMessageEventStream, Context, Model, ModelsApiStreamOptions, ModelsSimpleStreamOptions, ThinkingLevel } from '@earendil-works/pi-ai';

export type ConsolidationReasoning = 'off' | Extract<ThinkingLevel, 'minimal' | 'low' | 'medium'>;

/** Capabilities consumed by background distillation, independent of the Pi UI. */
export interface ModelRuntime {
  readonly model: Model<Api> | undefined;
  readonly modelRegistry: {
    find?(provider: string, modelId: string): Model<Api> | undefined;
    hasConfiguredAuth?(model: Model<Api>): boolean;
    streamSimple?(model: Model<Api>, context: Context, options?: ModelsSimpleStreamOptions): AssistantMessageEventStream;
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
