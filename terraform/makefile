.PHONY: apply destroy plan

apply: 
	terraform apply -var-file="credentials.tfvars"

destroy: 
	terraform destroy -var-file="credentials.tfvars"

plan: 
	terraform plan -var-file="credentials.tfvars"