# Scalable-Pipeline

Projeto criado para aplicar técnicas de paralelismo, concorrência e troca de mensagens no contexto de uma plataforma de recebimento de pedidos de delivery.

### Instruções de uso:

Primeiramente, em um terminal, rode o seguinte comando Docker para criar e ativar uma imagem do RabbitMQ:

``docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management``

Depois, para ativar todo o pipeline responsável pelo procedimento, rode os seguintes códigos em ordem:
    - `broker_server.py`
    - `broker_client.py`
    - `update_storage.py`
    - `update_quotes.py`

Por fim, para visualizar as informações no _dashboard_, rode o seguinto comando:

``streamlit run dashboard.py``