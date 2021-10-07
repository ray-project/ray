{{/*
Compute clusterMaxWorkers as the sum of per-pod-type max workers.
*/}}
{{- define "ray.clusterMaxWorkers" -}}
{{- $total := 0 }}
{{- range .Values.podTypes }}
{{- $total = add $total .maxWorkers }}
{{- end }}
{{- $total }}
{{- end }}
