import { DistillFailure } from './failure.ts';
import type { ConsolidationReasoning, ModelRuntime } from './runtime.ts';

export type ConsolidationSelection =
  | { readonly model: NonNullable<ModelRuntime['model']>; readonly reasoning?: ConsolidationReasoning; readonly error?: undefined }
  | { readonly model?: undefined; readonly reasoning?: undefined; readonly error: DistillFailure };

function unavailable(message: string): ConsolidationSelection {
  return { error: new DistillFailure('model_unavailable', message) };
}

function validIdentifier(value: string, maxLength: number): boolean {
  return value.length >= 1 && value.length <= maxLength && !/[\s\p{C}]/u.test(value);
}

/** Resolve process-local options against the live registry, never against a project file. */
export function selectConsolidationModel(context: ModelRuntime): ConsolidationSelection {
  const provider = process.env.PI_DISTILL_CONSOLIDATION_PROVIDER;
  const id = process.env.PI_DISTILL_CONSOLIDATION_MODEL;
  const reasoning = process.env.PI_DISTILL_CONSOLIDATION_REASONING;
  if (reasoning !== undefined && reasoning !== 'off' && reasoning !== 'minimal' && reasoning !== 'low' && reasoning !== 'medium') {
    return unavailable('PI_DISTILL_CONSOLIDATION_REASONING must be off, minimal, low, or medium (or unset).');
  }
  let model = context.model;
  if (provider !== undefined || id !== undefined) {
    if (provider === undefined || id === undefined || !validIdentifier(provider, 256) || !validIdentifier(id, 1024)) {
      return unavailable('Set both PI_DISTILL_CONSOLIDATION_PROVIDER and PI_DISTILL_CONSOLIDATION_MODEL to valid Pi identifiers, or unset both.');
    }
    try {
      model = context.modelRegistry.find?.(provider, id);
      if (model?.provider !== provider || model.id !== id || !context.modelRegistry.hasConfiguredAuth?.(model)) {
        return unavailable('The configured consolidation model is not registered in Pi or its provider has no configured authentication.');
      }
    } catch {
      return unavailable('Pi could not resolve the configured consolidation model.');
    }
  }
  if (model === undefined) return unavailable('No Pi chat model is selected for consolidation.');
  if (reasoning !== undefined && !context.modelRegistry.streamSimple) {
    return unavailable('The model runtime cannot apply configured consolidation reasoning.');
  }
  return { model, ...(reasoning === undefined ? {} : { reasoning }) };
}
