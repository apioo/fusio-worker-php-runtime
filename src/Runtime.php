<?php

namespace Fusio\Worker\Runtime;

use Fusio\Worker\About;
use Fusio\Worker\Execute;
use Fusio\Worker\Response;
use Fusio\Worker\ResponseHTTP;
use PSX\Record\Record;
use PSX\Schema\SchemaManager;
use PSX\Schema\SchemaTraverser;
use PSX\Schema\Visitor\TypeVisitor;

class Runtime
{
    public function get(): About
    {
        $about = new About();
        $about->setApiVersion('1.0.0');
        $about->setLanguage('php');

        return $about;
    }

    public function run(string $actionFile, \stdClass|Execute $payload): Response
    {
        if ($payload instanceof \stdClass) {
            $payload = $this->parseExecute($payload);
        }

        $connector = new Connector($payload->getConnections() ?? new Record());
        $dispatcher = new Dispatcher();
        $logger = new Logger();
        $responseBuilder = new ResponseBuilder();

        $handler = require $actionFile;
        if (!is_callable($handler)) {
            throw new \RuntimeException('Provided action does not return a callable');
        }

        call_user_func_array($handler, [
            $payload->getRequest(),
            $payload->getContext(),
            $connector,
            $responseBuilder,
            $dispatcher,
            $logger
        ]);

        $response = $responseBuilder->getResponse();
        if (!$response instanceof ResponseHTTP) {
            $response = new ResponseHTTP();
            $response->setStatusCode(204);
        }

        $return = new Response();
        $return->setEvents($dispatcher->getEvents());
        $return->setLogs($logger->getLogs());
        $return->setResponse($response);

        return $return;
    }

    private function parseExecute(\stdClass $payload): Execute
    {
        $schema = (new SchemaManager())->getSchema(Execute::class);
        $execute = (new SchemaTraverser())->traverse($payload, $schema, new TypeVisitor());

        if (!$execute instanceof Execute) {
            throw new \RuntimeException('Could not read execute payload');
        }

        return $execute;
    }
}
