data "aws_iam_role" "role_name" {
  name = var.role_name
}

data "aws_sfn_state_machine" "StockPyStateMachine" {
  name = "StockPyStateMachine"
}

resource "aws_cloudwatch_event_rule" "DailyStockPyEventBridge" {
  name                = "StockPyEventBridge"
  description         = "Rules to trigger Step Functions daily at 21:00 UTC"
  schedule_expression = "cron(00 21 * * ? *)"
}

resource "aws_cloudwatch_event_target" "target_step_functions" {
  rule     = aws_cloudwatch_event_rule.DailyStockPyEventBridge.name
  arn      = data.aws_sfn_state_machine.StockPyStateMachine.arn
  role_arn = data.aws_iam_role.role_name.arn

  input = jsonencode({})
}