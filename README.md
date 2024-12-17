# LINK DO VÍDEO YOUTUBE

**[Vídeo explicativo no youtube](https://youtu.be/M67wFBs1goM)**

# SOBRE O PROJETO

> *Info*: Se for rodar localmente, use o `poetry install` para garantir que tudo funcione como deveria.

### 1. Obtenção dos dados

#### Sobre os dados

A fonte de dados escolhida para este projeto foi a [GridStatus](https://www.gridstatus.io/) que disponibiliza uma plataforma analítica de dados sobre a indústria de energia. 

Dentre os vários dados disponíveis no site, foi escolhido o "*CAISO Fuel Mix*" onde temos dados, em intervalos de 5 minutos, sobre a geração de energia no estado da Califórnia. Mais informações [sobre a CAISO](https://en.wikipedia.org/wiki/California_Independent_System_Operator) e [sobre a fonte de dados utilizada](https://www.gridstatus.io/datasets/caiso_fuel_mix).

A tabela de dados possui informações sobre a geração de energia de diversas fontes:

- Solar
- **Eólica**
- Geotérmica
- Biomassa
- Biogás
- Hidro
- Carvão
- Nuclear
- Gás Natural
- Baterias
- Importação
- Outras

Pra a realização deste trabalho foi utilizada a energia eólica para um modelo e o restante das energias para o outro.

#### Download dos dados para treino do modelo de regressão

A consulta pode ser feita via API disponibilizada pela GridStatus. Para que as sazonalidades estejam representadas na amostra, foi feito download de um período de 5 anos - *de janeiro de 2019 a outubro de 2024*.

Por se tratar de uma extração única, o código foi criado, em Python, através de um script. Está disponível no GitHub do projeto no caminho `scripts/00_get_initial_data.ipynb`. 

A extração dos dados foi feita de janeiro de 2019 até outubro de 2014 e salva em formato deltalake no diretório `lake/delta_table` particionado por ano.

Para mais detalhes sobre, consultar o código.

### 2. Treinando o modelo

#### Escolha do modelo

Foi escolhido o LightGBM por conta de sua eficiência em trabalhar com dados regressivos para o primeiro modelo. Essa escolha foi definida a partir de um comparativo realizado com a ferramenta de automl "pycaret" para avaliação de benchmark entre mais de 15 modelos diferentes.
Já para o modelo de série temporal foi escolhido o Facebook Prophet, devido à facilidade de utilização através do framework próprio.

#### Preparação dos dados para o primeiro modelo

Como cada observação diz respeito à energia gerada dentro de um intervalo de 5 minutos e o objetivo é prever a próxima meia-hora de geração de energia eólica, **a janela de observação, ou o intervalo, será uma sequência de 6 observações**.

![modelo_regressao](docs/modelo_regressao.png)

Para preparar os dados para o treino do modelo, a série histórica foi dividida em vários intervalos subsequentes de 6 observações onde a próxima observação após esse intervalo é o valor esperado (*target*).

![dados_entrada](docs/dados_entrada.png)

O algoritmo analisará o comportamento de 6 observações para predizer qual será **o próximo valor**, ou seja, qual é a previsão da geração de energia eólica **dos próximos 5 minutos**.

Resumindo: O modelo receberá uma entrada com **6 observações**, ou seja, uma janela de 30 minutos de geração de energia para predizer qual será qual será a produção de energia dos **próximos 5 minutos**. 

Com os dados prontos, foi feita uma última divisão onde 80% das observações foram destinadas ao treino e 20% para o teste.

Para saber mais detalhes de toda a implementação, dê uma olhada nos códigos:

- `notebooks/02_criando_dados_treino_teste.ipynb`
- `notebooks/03_modelo_preditivo.ipynb`

### 3. Performance do modelo

O modelo foi selecionado utilizando 6 métricas para a avaliação, são elas: MAE, MSE, RMSE, R2, RMSLE e MAPE, além do tempo de treinamento/inferência. O LightGBM foi escolhido por apresentar o melhor "custo-benefício" ao analisar esses parâmetros. 
Segue tabela com resutados obtidos:

**exibir tabela exportada do Pycaret**

Para os modelos do Prophet, foi considerado que o modelo retornou um RMSE aceitável dentro do horizonte de cada modelo por período (hora, dia, mês) e a fácil utilização do framework para a entrega do projeto no tempo proposto.

### 4. Aplicação

O objetivo final do projeto é prever os próximos valores de geração de energia. 

#### 4.1 Modelo por 5 minutos
Para o primeiro modelo foi considerada somente a energia eólica, e a partir do input de 6 leituras anteriores, é possível prever a próximo leitura de geração de energia. A ideia do funcionamento está ilustrado abaixo:

![Predição meia-hora](docs/predicao_meia_hora.png)

#### 4.2 Modelos por periodos
Para o segundo tipo de modelos, a feature informada pelo usuário é o periodo de tempo, e foi treinado um modelo para cada energia em cada período dentre hora, dia e mês. 

#### 4.3 Front end
Para a visualização dos resultados, foi desenvolvida uma interface web com Streamlit para a visualização dos resutados e configurações do retorno. Segue abaixo exemplo de como essas configurações podem ser feitas:

![Configurações models por período](docs/configuracoes_por_periodo.png)

Além disso, na exibição das predições, é possível definir se deseja visualizar a predição em um mapa estático ou dinâmico, onde é possível selecionar ponto a ponto os valores reais e preditos retornados para o periodo de tempo do modelo. Segue abaixo exemplo de visualização das predições:

![Previsão exemplo](docs/previsao_wind_estatica.png)

### 4.4 Cloud
Para criar essa aplicação, foi criada uma arquitetura em Cloud utilizando os serviços da Amazon AWS composta pelos seguintes componentes (10 no total):

- `Lambda Function`
	- `getData`
	- `predictData`
	- `glueData`
- `Elastic Container Registry`
	- `tech-challenge-tres-get-data`
	- `tech-challenge-tres-glue-data`
	- `tech-challenge-tres-predict-data`
- `Event Bridge`
	- `getDataSchedule`
- `S3`
	- `alecrimtechchallengetresbronze`
	- `alecrimtechchallengetressilver`
	- `alecrimtechchallengetresgold`

Os arquivos com os modelos de regressão e o normalizador foram carregados no container `alecrimtechchallengetressilver` para uso das `Lambda Functions` como veremos a seguir.

O fluxo é o seguinte:

1. O `getDataSchedule` dispara um evento a cada 30 minutos
2. Esse evento é um gatilho que ativa a função `getData`
3. A função `getData` faz uma requisição à API do GridStatus dos dados da última meia-hora e salva no container `alecrimtechchallengetresbronze`
4. Ao salvar os dados, o container disparará um evento que, por sua vez, irá "*triggar*" a função `predictData`
5. A função `predictData` utilizará os últimos dados obtidos para predizer qual será a geração de energia eólica dos próximos 30 minutos utilizando o modelo de regressão treinado e armazenado no `alecrimtechchallengetresbronze`
6. Ao salvar os dados, o container disparará um evento que, por sua vez, irá "*triggar*" a última função `glueData`
7. A função `glueData` gerará um arquivo único contendo os dados das últimas 24h e os próximos 30 minutos preditos. Este arquivo será salvo no container `alecrimtechchallengetresgold`

![fluxo_aws](docs/fluxo_aws.png)

**NOTA: A extensão utilizada para armazenar os arquivos é "parquet".**

A implementação descrita pode ser justificada pela adoção de uma abordagem orientada a dados (data-driven) para apoiar a tomada de decisões no setor de energia renovável, especificamente no monitoramento e previsão da geração de energia eólica. Essa estrutura oferece benefícios significativos, incluindo:

1. **Tomada de Decisão Proativa e Otimizada**
    - A previsão da geração de energia eólica para os próximos 30 minutos permite que operadores de rede ou gestores de energia ajustem rapidamente a alocação de recursos.
    - Isso inclui decisões sobre compra e venda de energia no mercado spot, ativação de fontes de energia suplementares ou ajustes em contratos de fornecimento.

2. **Resiliência e Eficiência Operacional**
    - A geração de arquivos consolidados com os dados das últimas 24 horas e previsões futuras facilita o monitoramento contínuo e a identificação de padrões.
    - Esta visão integrada ajuda a detectar desvios, planejar manutenção preventiva e responder a eventos inesperados com maior rapidez.

3. **Apoio à Sustentabilidade e ao Cumprimento Regulatório**
    - A previsão precisa de geração eólica contribui para integrar fontes renováveis de forma eficiente na matriz energética, reduzindo a dependência de fontes não renováveis.
    - O histórico consolidado e as previsões podem ser utilizados para relatórios regulatórios e demonstração de conformidade ambiental.

4. **Automatização e Escalabilidade**
    - O pipeline automático descrito elimina processos manuais, reduzindo erros e permitindo que a solução escale para lidar com fluxos de dados maiores ou mais frequentes.
    - A arquitetura baseada em eventos oferece flexibilidade para adaptações e integrações com outras fontes de dados ou modelos preditivos.

5. **Melhoria Contínua e Feedback ao Modelo**
    - A coleta contínua de dados históricos e predições fornece um ciclo de feedback para aprimorar os modelos preditivos ao longo do tempo, aumentando a acurácia e a confiabilidade.

Essa abordagem não apenas promove uma operação mais eficiente e sustentável, mas também alinha a estratégia de gerenciamento energético com princípios de inovação tecnológica e inteligência artificial.

Para saber mais detalhes de toda a implementação, dê uma olhada nos códigos:

- `lambda_functions/get_data/lambda_function.py`
- `lambda_functions/predict_data/lambda_function.py`
- `lambda_functions/glue_data/lambda_function.py`
