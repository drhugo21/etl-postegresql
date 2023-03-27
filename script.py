import docker

client = docker.from_env()

result = client.containers.get('refera-data-engineer-short-challenge_transactional_1').exec_run('pg_dump -U postgres -F c -b -v -f /tmp/backup.dump dvdrental')

if result.exit_code == 0:
    print('Dump do banco de dados Transacional criado com sucesso!')
else:
    print('Erro ao criar o dump do banco de dados Transacional!')

container_A = client.containers.get('refera-data-engineer-short-challenge_transactional_1')
container_B = client.containers.get('refera-data-engineer-short-challenge_analytics_1')

stream, stat = container_A.get_archive('/tmp/backup.dump')
data = b"".join(stream)

with open('/tmp/backup.dump', 'wb') as f:
    f.write(data)

with open('/tmp/backup.dump', 'rb') as f:
    container_B.put_archive('/tmp', f.read())

result = container_B.exec_run('pg_restore -U postgres -d analytics /tmp/backup.dump')

if result.exit_code == 0:
    print('Banco de dados Analitico restaurado com sucesso!')
else:
    print('Erro ao restaurar o banco de dados Analitico!')

client.close()
