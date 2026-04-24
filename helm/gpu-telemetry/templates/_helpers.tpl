{{/*
Expand the name of the chart.
*/}}
{{- define "gpu-telemetry.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name (chart name).
*/}}
{{- define "gpu-telemetry.fullname" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "gpu-telemetry.labels" -}}
app.kubernetes.io/part-of: {{ include "gpu-telemetry.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for a specific component.
Usage: {{ include "gpu-telemetry.selectorLabels" (dict "component" "broker") }}
*/}}
{{- define "gpu-telemetry.selectorLabels" -}}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/part-of: gpu-telemetry
{{- end }}

{{/*
Container image string.
*/}}
{{- define "gpu-telemetry.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag }}
{{- end }}

{{/*
Namespace helper — creates namespace if it doesn't already exist.
*/}}
{{- define "gpu-telemetry.namespace" -}}
{{ .Values.global.namespace }}
{{- end }}
