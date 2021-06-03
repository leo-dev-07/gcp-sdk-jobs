# gcp-sdk-jobs
Python codes to user GCP SDK
_**`Este arquivo visa explicar como implementar uma solução OLAP usando os SDKs de Storage e do BigQuery da Google.`**_

Razão da arquitetura escolhida:
    
    Foi escolhida uma arquitetura ETL totalmente codificada pelo fato do problema ser pequeno, não exiginto atualização de tabelas e nem método
    de paralelismo computacional para carregar ou processar os dados.
    
    A ferramenta escolhida para guardar e analisar os dados foi o BigQuery. Ele nos dá fácil acesso e velocidade no processamento das consultas. Sem falar que ele
    tem uma exelente integração com a solução dada via SDK.
    
    Para Exibir os dados graficamente foi escolhido o Google Data Studio. É uma ferramenta gratuita e com muitos recursos fáceis de serem utilizados
    
Abaixo vai todos os paços da implementação:

    O primeiro e primordial passo é configurar um ambiente de desenvolvimento Python para seu projeto.

    1- Tudo começa com a instalação da SDK da Google que é encontrado neste link. https://cloud.google.com/sdk/docs/downloads-interactive
    2- Configure o SKD De sua máquina para acessar sua Conta usando este link. https://cloud.google.com/sdk/docs/initializing
    3- Configure uma o usuário do seu projeto na AIM para ter uma conta de serviço: https://cloud.google.com/iam/docs/service-accounts
    4- Crie ma chave e coloque-a em sua vaiável de ambiente: https://cloud.google.com/iam/docs/creating-managing-service-account-keys/?hl=pt-br

Depois de seguir estes passos, você já estará pronto para começar a codificar usando o SDK da Google Cloud.

O código tem funções de: 
`    .Criação de buckets
     .Ingestão de dados neles
     .Criação de schema automático
     .Criação de tabela
     .Inserção de dados nas tabelas
     .SQL Demo com análises para teste do BD`
 
