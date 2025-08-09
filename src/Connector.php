<?php

namespace Fusio\Worker\Runtime;

use Fusio\Engine\ConnectorInterface;
use Fusio\Worker\ExecuteConnection;
use Fusio\Worker\Runtime\Exception\ConnectionException;
use Fusio\Worker\Runtime\Exception\ConnectionNotFoundException;
use Fusio\Worker\Runtime\Exception\InvalidConnectionTypeException;
use Fusio\Worker\Runtime\Exception\RuntimeException;
use PSX\Record\Record;

class Connector implements ConnectorInterface
{
    private Record $connections;
    private array $instances;

    public function __construct(Record $connections)
    {
        $this->connections = $connections;
        $this->instances = [];
    }

    /**
     * @throws RuntimeException
     */
    public function getConnection(string|int $connectionId): mixed
    {
        $name = (string) $connectionId;

        if (isset($this->instances[$name])) {
            return $this->instances[$name];
        }

        if (!$this->connections->containsKey($name)) {
            throw new ConnectionNotFoundException('Connection ' . $name . ' does not exist');
        }

        /** @var ExecuteConnection $connection */
        $connection = $this->connections->get($name);
        $config = \json_decode(\base64_decode($connection->getConfig() ?? ''));

        return $this->instances[$name] = match ($connection->getType()) {
            'Fusio.Adapter.Sql.Connection.Sql' => $this->newSqlConnectionSql($config),
            'Fusio.Adapter.Sql.Connection.SqlAdvanced' => $this->newSqlConnectionSqlAdvanced($config),
            'Fusio.Adapter.Http.Connection.Http' => $this->newHttpConnectionHttp($config),
            'Fusio.Adapter.Mongodb.Connection.MongoDB' => $this->newMongodbConnectionMongoDB($config),
            'Fusio.Adapter.Elasticsearch.Connection.Elasticsearch' => $this->newElasticsearchConnectionElasticsearch($config),
            'Fusio.Adapter.SdkFabric.Connection.Airtable' => $this->newSdkFabricConnectionAirtable($config),
            'Fusio.Adapter.SdkFabric.Connection.Discord' => $this->newSdkFabricConnectionDiscord($config),
            'Fusio.Adapter.SdkFabric.Connection.Notion' => $this->newSdkFabricConnectionNotion($config),
            'Fusio.Adapter.SdkFabric.Connection.Starwars' => $this->newSdkFabricConnectionStarwars(),
            'Fusio.Adapter.SdkFabric.Connection.Twitter' => $this->newSdkFabricConnectionTwitter($config),
            default => throw new InvalidConnectionTypeException('Provided a not supported connection type: ' . $connection->getType()),
        };
    }

    private function newSqlConnectionSql(\stdClass $config): \Doctrine\DBAL\Connection
    {
        $params = [
            'dbname'   => $config->database ?? null,
            'user'     => $config->username ?? null,
            'password' => $config->password ?? null,
            'host'     => $config->host ?? null,
            'driver'   => $config->type ?? null,
        ];

        try {
            return \Doctrine\DBAL\DriverManager::getConnection($params);
        } catch (\Doctrine\DBAL\Exception $e) {
            throw new ConnectionException('Could not establish connection', previous: $e);
        }
    }

    private function newSqlConnectionSqlAdvanced(\stdClass $config): \Doctrine\DBAL\Connection
    {
        $params = (new \Doctrine\DBAL\Tools\DsnParser())->parse($config->url ?? '');

        try {
            return \Doctrine\DBAL\DriverManager::getConnection($params);
        } catch (\Doctrine\DBAL\Exception $e) {
            throw new ConnectionException('Could not establish connection', previous: $e);
        }
    }

    private function newHttpConnectionHttp(\stdClass $config): \GuzzleHttp\Client
    {
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

        return new \GuzzleHttp\Client($options);
    }

    private function newMongodbConnectionMongoDB(\stdClass $config): \MongoDB\Database
    {
        $client = new \MongoDB\Client($config->url);
        return $client->selectDatabase($config->database);
    }

    private function newElasticsearchConnectionElasticsearch(\stdClass $config): \Elasticsearch\Client
    {
        return \Elasticsearch\ClientBuilder::create()
            ->setHosts(explode(',', $config->host))
            ->build();
    }

    private function newSdkFabricConnectionAirtable(\stdClass $config): \SdkFabric\Airtable\Client
    {
        return \SdkFabric\Airtable\Client::build($config->access_token ?? '');
    }

    private function newSdkFabricConnectionDiscord(\stdClass $config): \SdkFabric\Discord\Client
    {
        return \SdkFabric\Discord\Client::build($config->access_token ?? '');
    }

    private function newSdkFabricConnectionNotion(\stdClass $config): \SdkFabric\Notion\Client
    {
        return \SdkFabric\Notion\Client::build($config->access_token ?? '');
    }

    private function newSdkFabricConnectionStarwars(): \SdkFabric\Starwars\Client
    {
        return \SdkFabric\Starwars\Client::buildAnonymous();
    }

    private function newSdkFabricConnectionTwitter(\stdClass $config): \SdkFabric\Twitter\Client
    {
        return \SdkFabric\Twitter\Client::build($config->access_token ?? '');
    }
}
