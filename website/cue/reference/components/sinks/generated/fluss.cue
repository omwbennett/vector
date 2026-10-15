package metadata

generated: components: sinks: fluss: configuration: {
	acknowledgements: {
		description: """
			Controls how acknowledgements are handled for this sink.

			See [End-to-end Acknowledgements][e2e_acks] for more information on how event acknowledgement is handled.

			[e2e_acks]: https://vector.dev/docs/architecture/end-to-end-acknowledgements/
			"""
		required: false
		type:     _schemaDefinitions["bool_or_struct::vector_core::config::AcknowledgementsConfig"]
	}
	batch_size: {
		description: "Writer batch size in bytes."
		required:    false
		type: int: default: 2097152
	}
	batch_timeout_ms: {
		description: "Writer batch timeout in milliseconds."
		required:    false
		type: int: default: 100
	}
	bootstrap_servers: {
		description: """
			Fluss coordinator bootstrap server address.

			Must be in the form of `host:port`.
			"""
		required: true
		type: string: examples: ["127.0.0.1:9123"]
	}
	buffer_memory_size: {
		description: "Writer buffer memory size in bytes."
		required:    false
		type: uint: default: 67108864
	}
	buffer_wait_timeout_ms: {
		description: "Maximum time spent waiting for native writer buffer space, in milliseconds."
		required:    false
		type: uint: default: 1000
	}
	columns: {
		description: "Column mappings (only used when schema_mode is \"manual\")."
		required:    false
		type: array: {
			default: []
			items: type: object: options: {
				data_type: {
					description: "Fluss data type for this column."
					required:    true
					type: string: enum: {
						array:        "Array type (currently unsupported; rejected rather than serialized as a string)."
						bigint:       "64-bit signed integer."
						binary:       "Fixed-length byte array."
						boolean:      "Boolean value."
						bytes:        "Raw byte array."
						char:         "Fixed-length string."
						date:         "Date (days since epoch)."
						decimal:      "Decimal with precision and scale."
						double:       "64-bit floating point."
						float:        "32-bit floating point."
						int:          "32-bit signed integer."
						map:          "Map type (currently unsupported; rejected rather than serialized as a string)."
						smallint:     "16-bit signed integer."
						string:       "UTF-8 string."
						time:         "Time (milliseconds since midnight)."
						timestamp:    "Timestamp without timezone (milliseconds since epoch)."
						timestampltz: "Timestamp with local timezone (milliseconds since epoch)."
						tinyint:      "8-bit signed integer."
					}
				}
				from_field: {
					description: "Path to the field in the Vector event (e.g., \".message\", \".timestamp\")."
					required:    true
					type: string: examples: [".message", ".timestamp"]
				}
				to_column: {
					description: "Column name in the Fluss table."
					required:    true
					type: string: examples: ["message", "event_time"]
				}
			}
		}
	}
	connect_timeout_ms: {
		description: "Connection timeout in milliseconds."
		required:    false
		type: uint: default: 120000
	}
	database: {
		description: "Database name in Fluss."
		required:    true
		type: string: examples: [
			"fluss"
		]
	}
	max_cached_tables: {
		description: """
			Maximum cached tables. Reaching this limit drains and replaces the connection,
			also bounding the native client's table and writer caches.
			"""
		required: false
		type: uint: default: 128
	}
	max_in_flight: {
		description: """
			Maximum events awaiting submission or server acknowledgement.
			Vector's driver may additionally prefetch up to 1024 events.
			"""
		required: false
		type: uint: default: 256
	}
	request_timeout_ms: {
		description: """
			Submission retries and delivery deadline per event, in milliseconds.
			An already running append may exceed this by buffer_wait_timeout_ms.
			Expired writes have unknown delivery status and are not submitted again.
			"""
		required: false
		type: uint: default: 60000
	}
	sasl_password: {
		description: "SASL authentication password."
		required:    false
		type: string: examples: ["password"]
	}
	sasl_username: {
		description: "SASL authentication username."
		required:    false
		type: string: examples: [
			"user"
		]
	}
	schema_mode: {
		description: "Schema mode: \"auto\" to discover schema from Fluss, \"manual\" to use column mappings."
		required:    false
		type: string: {
			default: "auto"
			enum: {
				auto: """
					Auto-discover schema from Fluss table metadata.
					Fields are mapped by name; types are inferred from the table schema.
					"""
				manual: "Manually specify column mappings."
			}
		}
	}
	schema_refresh_interval_sec: {
		description: "How often to refresh table metadata, in seconds, in either schema mode."
		required:    false
		type: uint: default: 60
	}
	shutdown_timeout_ms: {
		description: "Deadline for draining and closing after the input ends, in milliseconds."
		required:    false
		type: uint: default: 30000
	}
	table: {
		description: """
			The Fluss table name to write events to.

			Supports templates for dynamic routing (e.g., `{{ .table }}`).
			"""
		required: true
		type: string: {
			examples: ["events", "{{ .table }}"]
			syntax: "template"
		}
	}
	table_field: {
		description: """
			Event field removed after rendering the target table name.
			Useful for routing fields in dynamic table templates.
			"""
		required: false
		type: string: examples: [
			"table"
		]
	}
}
