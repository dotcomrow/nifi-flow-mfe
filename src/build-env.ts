declare const __MFE_PREVIEW_AUTH_GATEWAY_URL__: string;
declare const __MFE_PREVIEW_AUTH_APP_SLUG__: string;
declare const __MFE_PREVIEW_AUTH_CODE_PARAM__: string;
declare const __MFE_BUILD_VERSION__: string;
declare const __MFE_BUILD_COMMIT__: string;
declare const __MFE_BUILD_TIMESTAMP__: string;
declare const __MFE_BUILD_MODE__: string;

function asDefault(value: string | undefined): string {
  if (typeof value !== "string") {
    return "";
  }
  return value.trim();
}

export const buildEnvDefaults = {
  previewAuthGatewayUrl: asDefault(__MFE_PREVIEW_AUTH_GATEWAY_URL__),
  previewAuthAppSlug: asDefault(__MFE_PREVIEW_AUTH_APP_SLUG__),
  previewAuthCodeParam: asDefault(__MFE_PREVIEW_AUTH_CODE_PARAM__),
  buildVersion: asDefault(__MFE_BUILD_VERSION__),
  buildCommit: asDefault(__MFE_BUILD_COMMIT__),
  buildTimestamp: asDefault(__MFE_BUILD_TIMESTAMP__),
  buildMode: asDefault(__MFE_BUILD_MODE__),
};
