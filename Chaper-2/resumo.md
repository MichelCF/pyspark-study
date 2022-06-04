# Resumo capítulo 2
## MapReduce
Um workflow comum começa com alguma transformação lógica (mapping phase). Esse processo nada mais é que extrair as informações do sistema de arquivos distribuído, e é dado um código para cada arquivo, depois os arquivos mapeados são adicionados em uma estrutura chave/valor.
O resultado da mapping phase é passado para a próxima etapa, o reduce. O Reduce basicamente ordena e agrupa os arquivos por chaves comuns.
Em resumo, O padrão de execução MapReduce pode ser pensado como uma abordagem distribuída de dividir e conquistar, para lidar com conjuntos de dados cada vez maiores.
Mais máquinas podem ser adicionadas para compartilhar o trabalho horizontalmente. Cada tarefa pode ser tratada conceitualmente como uma caixa preta, que recebe uma entrada, processa a entrada e salva o resultado em um local específico. 

## RDD Model
Podemos pensar no RDD como uma coleção imutável de dados que podem apenas serem lidos e são particionados em um conjunto de servidores conectados à rede que estão vinculados a um Aplicativo Spark, chamado executor.

## Elementos básicos de uma aplicação spark.

### Driver Process:
 O programa de driver é simplesmente o aplicativo Apache Spark. Cada aplicação é
supervisionado pelo programa driver, e as etapas de execução (o trabalho) são divididas
e distribuído entre os executores do programa usando comunicação RPC simples para
cada estágio de execução, normalmente algum tipo de transformação, ao longo da jornada para o resultado desejado do aplicativo, que é referido como uma ação.

### Cluster Manager:
Como o Spark é executado em diversos nodes, usamos um programa para gerenciar esses nodes. Esse é o papel do cluster manager. Ele verifica coisas como a saúde dos nodes, a carga de trabalho entre muitas outras coisas.

### Executors:
Responsável por armazenar os dados e pelo processamento dos dados.

