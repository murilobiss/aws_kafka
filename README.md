# AWS Kafka Integration

Este repositório contém scripts e configurações para integrar o Apache Kafka com os serviços da AWS, incluindo a configuração e o gerenciamento de clusters Kafka na AWS. O projeto tem como objetivo facilitar a implementação de soluções de streaming e processamento de dados em tempo real, utilizando a infraestrutura da AWS e a robustez do Apache Kafka.

## Funcionalidades

- **Configuração do Kafka na AWS**: Automatiza a criação e configuração de clusters Kafka na AWS, incluindo a definição de tópicos, brokers e consumidores.
- **Gerenciamento de Tópicos e Partições**: Ferramentas para gerenciar tópicos e partições no Kafka.
- **Integração com AWS Services**: Integração direta com serviços AWS como S3, CloudWatch, e IAM para monitoramento e escalabilidade.
- **Segurança**: Implementação de configurações de segurança para Kafka, incluindo autenticação e autorização via AWS IAM.

## Pré-requisitos

Antes de executar o projeto, certifique-se de que o seguinte está configurado em seu ambiente:

- Conta na AWS com permissões adequadas para criar e gerenciar recursos como EC2, S3, IAM, e CloudWatch.
- **Kafka** instalado localmente ou configurado na AWS.
- Ferramentas de linha de comando da AWS (`aws-cli`) instaladas e configuradas.
- Java 8+ e Apache Kafka.

## Estrutura do Repositório

O repositório está organizado da seguinte forma:

```
aws_kafka/
│
├── config/
│   └── kafka-config.yaml    # Arquivo de configuração do Kafka
│
├── scripts/
│   ├── create-cluster.sh    # Script para criação do cluster Kafka na AWS
│   ├── manage-topics.sh     # Script para gerenciar tópicos Kafka
│   └── monitor.sh           # Script para monitoramento do Kafka com CloudWatch
│
└── README.md                # Este arquivo
```

## Instalação

### Passo 1: Clone o Repositório

Clone este repositório para o seu ambiente local:

```bash
git clone https://github.com/murilobiss/aws_kafka.git
cd aws_kafka
```

### Passo 2: Configuração

1. **Configuração do Kafka**: Edite o arquivo `config/kafka-config.yaml` para definir os parâmetros específicos do seu ambiente Kafka na AWS, como brokers e tópicos.
   
2. **Permissões AWS**: Assegure-se de que as credenciais da AWS estão configuradas corretamente. Você pode configurar as credenciais usando o comando:

```bash
aws configure
```

### Passo 3: Executar Scripts

Execute os scripts conforme necessário para configurar, gerenciar e monitorar seu cluster Kafka:

- Para criar um cluster Kafka na AWS:

```bash
bash scripts/create-cluster.sh
```

- Para gerenciar tópicos:

```bash
bash scripts/manage-topics.sh
```

- Para monitorar o Kafka com o CloudWatch:

```bash
bash scripts/monitor.sh
```