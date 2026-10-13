# Company telemetry policies, checked by `weaver registry live-check` on every sample.
# Requirement levels are enforced by the registry and the matchers in .weaver.toml; these rules
# cover what a requirement level can't express.
package live_check_advice

import rego.v1

log_attr(name) := value if {
	some a in input.sample.log.attributes
	a.name == name
	value := a.value
}

is_wide_event if log_attr("main") == true

# A wide event's outcome must agree with its HTTP status code.
deny contains company_advice("wide_event_outcome_status_mismatch", "violation", context, message) if {
	input.sample.log
	is_wide_event
	outcome := log_attr("outcome")
	status := log_attr("http.response.status_code")
	not status_allowed(outcome, status)
	context := {"outcome": outcome, "http.response.status_code": status}
	message := sprintf("Wide event has outcome '%s' but status code %v.", [outcome, status])
}

status_allowed("success", status) if {
	status >= 200
	status < 300
}

status_allowed(outcome, status) if {
	outcome == "payment_pending"
	status == 202
}

status_allowed(outcome, status) if {
	outcome in {"upstream_error", "db_error"}
	status >= 500
}

# A wide event that did not succeed must be logged at WARN or above, and a successful one below.
deny contains company_advice("wide_event_severity_mismatch", "violation", context, message) if {
	input.sample.log
	is_wide_event
	outcome := log_attr("outcome")
	severity := input.sample.log.severity_number
	succeeded := outcome == "success"
	below_warn := severity < 13
	succeeded != below_warn
	context := {"outcome": outcome, "severity_number": severity}
	message := sprintf("Wide event with outcome '%s' has severity %v; use WARN (13) or above exactly when the outcome is not success.", [outcome, severity])
}

# CPU time can't exceed wall-clock time: requests are handled on one thread.
deny contains company_advice("wide_event_cpu_exceeds_duration", "violation", context, message) if {
	input.sample.log
	is_wide_event
	cpu := log_attr("cpu_ms")
	duration := log_attr("duration_ms")
	cpu > duration
	context := {"cpu_ms": cpu, "duration_ms": duration}
	message := sprintf("Wide event reports cpu_ms %v, more than its duration_ms %v.", [cpu, duration])
}

# The trace_id and span_id attributes must repeat the record's own trace context.
deny contains company_advice("log_trace_context_mismatch", "violation", context, message) if {
	input.sample.log
	some field in ["trace_id", "span_id"]
	value := log_attr(field)
	value != input.sample.log[field]
	context := {"attribute_key": field, "attribute_value": value, "record_value": input.sample.log[field]}
	message := sprintf("Attribute '%s' is '%s' but the record's %s is '%s'.", [field, value, field, input.sample.log[field]])
}

# Telemetry identifies users by user.id only. Names and email addresses are personal data.
deny contains company_advice("personal_data", "violation", context, message) if {
	input.sample.attribute
	input.sample.attribute.name in {"user.email", "user.full_name", "user.name"}
	context := {"attribute_key": input.sample.attribute.name}
	message := sprintf("Attribute '%s' is personal data; identify users by user.id.", [input.sample.attribute.name])
}

company_advice(advice_type, advice_level, advice_context, message) := {
	"type": "advice",
	"advice_type": advice_type,
	"advice_level": advice_level,
	"advice_context": advice_context,
	"message": message,
}
