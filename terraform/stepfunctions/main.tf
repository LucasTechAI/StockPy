data "aws_iam_role" "role_name" {
  name = var.role_name

}

locals {
  state_machine_definition = file("${path.module}/${var.state_machine_definition_file}")
}

resource "aws_sfn_state_machine" "stock_py_state_machine" {
  name     = "StockPyStateMachine"
  role_arn = data.aws_iam_role.role_name.arn
  definition = local.state_machine_definition
}
