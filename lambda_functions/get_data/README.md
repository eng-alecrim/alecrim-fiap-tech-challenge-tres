- [Fonte](https://docs.aws.amazon.com/lambda/latest/dg/python-image.html)

1. Insira as infos do laboratório em `~/.aws/credentials` e declare as variáveis de ambiente

```shell
export USER_ID=<USER_ID>
export ECR_REPO_NAME=<ECR_REPO_NAME>
export LAMBDA_FUNCTION_NAME=getData
```

2. Faça o login no ECR

```shell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${USER_ID}.dkr.ecr.us-east-1.amazonaws.com
```

3. Crie um repositório onde ficará as imgs

```shell
aws ecr create-repository --repository-name ${ECR_REPO_NAME} --region us-east-1 --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
```

Resposta pós-comando:

```shell
{
    "repository": {
        "repositoryArn": "arn:aws:ecr:us-east-1:************:repository/****************************",
        "registryId": "************",
        "repositoryName": "****************************",
        "repositoryUri": "************.dkr.ecr.us-east-1.amazonaws.com/****************************",
        "createdAt": "2024-11-22T23:00:43.227000-03:00",
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": true
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}
(END)
```

4. Construa a img e pusha pra ECR

```shell
docker build --platform linux/amd64 -t ${ECR_REPO_NAME}:latest .
docker tag ${ECR_REPO_NAME}:latest ${USER_ID}.dkr.ecr.us-east-1.amazonaws.com/${ECR_REPO_NAME}:latest
docker push ${USER_ID}.dkr.ecr.us-east-1.amazonaws.com/${ECR_REPO_NAME}:latest
```

5. Crie uma lambda function em cima da img no ECR

```shell
aws lambda create-function \
  --function-name ${LAMBDA_FUNCTION_NAME} \
  --package-type Image \
  --code ImageUri=${USER_ID}.dkr.ecr.us-east-1.amazonaws.com/${ECR_REPO_NAME}:latest \
  --role arn:aws:iam::${USER_ID}:role/LabRole \
  --timeout 360
```

Resposta do comando:

```shell
{
    "FunctionName": "****************************",
    "FunctionArn": "arn:aws:lambda:us-east-1:************:function:****************************",
    "Role": "arn:aws:iam::************:role/LabRole",
    "CodeSize": 0,
    "Description": "",
    "Timeout": 3,
    "MemorySize": 128,
    "LastModified": "2024-11-23T02:08:16.931+0000",
    "CodeSha256": "****************************************************************",
    "Version": "$LATEST",
    "TracingConfig": {
        "Mode": "PassThrough"
    },
    "RevisionId": "************************************",
    "State": "Pending",
    "StateReason": "The function is being created.",
    "StateReasonCode": "Creating",
    "PackageType": "Image",
    "Architectures": [
        "x86_64"
    ],
    "EphemeralStorage": {
        "Size": 512
    },
    "SnapStart": {
        "ApplyOn": "None",
        "OptimizationStatus": "Off"
    },
    "LoggingConfig": {
        "LogFormat": "Text",
        "LogGroup": "/aws/lambda/****************************"
    }
}
(END)
```

6. Vá ao painel de controle da AWS e mexa em algumas configs
	- Variáveis de ambiente
		- `GRIDSTATUS_API_KEY`