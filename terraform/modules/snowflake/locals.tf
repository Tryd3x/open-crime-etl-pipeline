locals {
  wh_size = "XSMALL"
  auto_suspend = 300 # in seconds
  auto_resume = true
  initially_suspended = true

  min_cluster = 2
  max_cluster = 4
  scaling_policy = "ECONOMY"
}