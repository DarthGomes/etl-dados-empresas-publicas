# 🏢 ETL Dados Empresas Públicas

## 📊 Visão Geral

Este projeto realiza o ETL (Extração, Transformação e Carga) de dados públicos de empresas, disponibilizados pela Receita Federal do Brasil. Utiliza Python com Polars para manipulação eficiente de grandes conjuntos de dados.

## 🚀 Funcionalidades

- 📥 Download automático de arquivos de dados CNPJ
- 🗜️ Descompactação de arquivos ZIP
- 🧹 Limpeza e processamento de dados
- 🔗 Junção de diferentes conjuntos de dados
- 📈 Análise e categorização de empresas

## 🛠️ Tecnologias Utilizadas

- Python 3.12+
- Poetry para gerenciamento de dependências
- Polars para processamento de dados
- httpx para requisições HTTP
- tqdm para barras de progresso
- backoff para retentativas
- Ferramentas de linting e formatação: ruff, blue, isort


## 🚀 Como Usar

1. Clone o repositório
2. Instale o Poetry: `pip install poetry`
3. Instale as dependências: `poetry install`
4. Inicie o shell do poetry: `poetry shell`
5. Execute o script principal: `poetry run python src.py`

## 📋 Processo

1. 📥 Download de dados da Receita Federal
2. 🗜️ Descompactação dos arquivos
3. 🧹 Limpeza e processamento dos dados
4. 🔗 Junção dos diferentes conjuntos de dados
5. 📊 Geração do arquivo final `result.csv`

## 📈 Resultado

O script gera um arquivo CSV (`result.csv`) contendo informações detalhadas sobre empresas.

## ⚠️ Notas

- Este script foi desenvolvido com propósitos educacionais e de estudo.
- Os dados utilizados são de domínio público e podem ser acessados através do portal da Receita Federal no seguinte link: [Cadastro Nacional da Pessoa Jurídica - CNPJ](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- O script lida com grandes volumes de dados. Certifique-se de ter espaço em disco suficiente.
- O tempo de execução pode variar dependendo da sua conexão de internet e do poder de processamento do seu computador.
- Sinta-se à vontade para fazer modificações locais no script para adaptá-lo às necessidades específicas do seu projeto ou estudo.
- Lembre-se de respeitar os termos de uso e as políticas de privacidade relacionados aos dados públicos utilizados.
## 🧪 Testes

TODO
