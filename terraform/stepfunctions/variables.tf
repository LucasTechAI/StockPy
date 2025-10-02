variable "role_name" {
  default = "LabRole"
}

variable "state_machine_definition_file" {
  description = "Path to the state machine definition JSON file"
  type        = string
  default     = "state_machine_definition.json"
}