<?php

namespace Fusio\Worker\Runtime;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;
use Elasticsearch\ClientBuilder;
use Fusio\Worker\ExecuteConnection;
use GuzzleHttp\Client;
use PSX\Record\Record;

class Connector
{
    private Record $connections;
    private array $instances;

    public function __construct(Record $connections)
    {
        $this->connections = $connections;
        $this->instances = [];
    }

    public function getConnection(string $name): mixed
    {
        if (isset($this->instances[$name])) {
            return $this->instances[$name];
        }

        if (!$this->connections->containsKey($name)) {
            throw new \RuntimeException('Connection does not exist');
        }

        /** @var ExecuteConnection $connection */
        $connection = $this->connections->get($name);
        $config = \json_decode(\base64_decode($connection->getConfig() ?? ''));

        if ($connection->getType() === 'Fusio.Adapter.Sql.Connection.Sql') {
            $database = $config->database ?? null;
            $username = $config->username ?? null;
            $password = $config->password ?? null;
            $host = $config->host ?? null;
            $type = $config->type ?? null;

            return $this->instances[$name] = DriverManager::getConnection([
                'dbname'   => $database,
                'user'     => $username,
                'password' => $password,
                'host'     => $host,
                'driver'   => $type,
            ]);
        } else if ($connection->getType() === 'Fusio.Adapter.Sql.Connection.SqlAdvanced') {
            $params = (new DsnParser())->parse($config->url ?? '');
            return $this->instances[$name] = DriverManager::getConnection($params);
        } else if ($connection->getType() === 'Fusio.Adapter.Http.Connection.Http') {
            $options = [];

            $baseUri = $config->url ?? null;
            if ($baseUri !== null) {
                $options['base_uri'] = $baseUri;
            }

            $username = $config->username ?? null;
            $password = $config->password ?? null;
            if ($username !== null && $password !== null) {
                $options['auth'] = [$username, $password];
            }

            $proxy = $config->proxy ?? null;
            if ($proxy !== null) {
                $options['proxy'] = $proxy;
            }

            $options['http_errors'] = false;

            return $this->instances[$name] = new Client($options);
        } else if ($connection->getType() === 'Fusio.Adapter.Mongodb.Connection.MongoDB') {
            $client = new \MongoDB\Client($config->url);
            $database = $client->selectDatabase($config->database);

            return $this->instances[$name] = $database;
        } else if ($connection->getType() === 'Fusio.Adapter.Elasticsearch.Connection.Elasticsearch') {
            $client = ClientBuilder::create()
                ->setHosts(explode(',', $config->host))
                ->build();

            return $this->instances[$name] = $client;
        } else {
            throw new \RuntimeException('Provided a not supported connection type: ' . $connection->getType());
        }
    }
}