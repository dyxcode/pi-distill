export const evaluationModelId = process.env.PI_DISTILL_EVAL_MODEL ?? "gemini-3.8-flash";

export async function selectEvaluationModel(runtime, requestedModel = evaluationModelId) {
  if (!["gemini-3.8-flash", "gemini-3.1-flash-lite"].includes(requestedModel)) {
    throw new Error("Evaluation model must have a verified free tier");
  }
  const signal = AbortSignal.timeout(15_000);
  // A signal makes credential lock failures observable. The synchronous auth
  // snapshot is intentionally not consulted: it is empty before SDK refresh.
  const credentials = await runtime.listCredentials({ signal });
  const credentialProviders = credentials.map(({ providerId, type }) => ({ providerId, type }));
  const available = await runtime.getAvailable("google", { signal });
  const model = available.find((item) => item.provider === "google" && item.id === requestedModel);
  return { model, credentialProviders, ...(model ? {} : { failure: "requested_model_unavailable" }) };
}
