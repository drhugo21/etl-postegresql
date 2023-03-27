import docker

# Criacao da instancia no Docker
client = docker.from_env()

# Criacao do dump
result = client.containers.get('etl-postegresql_transactional_1').exec_run('pg_dump -U postgres -F c -b -v -f /tmp/backup.dump dvdrental')

# Verificacao se o dump deu certo
if result.exit_code == 0:
    print('Dump do banco de dados Transacional criado com sucesso!')
else:
    print('Erro ao criar o dump do banco de dados Transacional!')

# Pegando o id dos containers
container_A = client.containers.get('etl-postegresql_transactional_1')
container_B = client.containers.get('etl-postegresql_analytics_1')

# Criacao do path para copia do arquivo
stream, stat = container_A.get_archive('/tmp/backup.dump')
data = b"".join(stream)

with open('/tmp/backup.dump', 'wb') as f:
    f.write(data)

# Colando o arquivo no container analitico
with open('/tmp/backup.dump', 'rb') as f:
    container_B.put_archive('/tmp', f.read())

# Restaurando dump no container analitico
result = container_B.exec_run('pg_restore -U postgres -d analytics /tmp/backup.dump')

# Verificando se codigo deu certo
if result.exit_code == 0:
    print('Banco de dados Analitico restaurado com sucesso!')
else:
    print('Erro ao restaurar o banco de dados Analitico!')

# Fechando conexao
client.close()
