<?php

namespace Fusio\Worker\Runtime;

use Fusio\Worker\About;
use Fusio\Worker\Execute;
use Fusio\Worker\Response;
use Fusio\Worker\ResponseHTTP;
use Fusio\Worker\Runtime\Exception\FileNotFoundException;
use Fusio\Worker\Runtime\Exception\InvalidActionException;
use Fusio\Worker\Runtime\Exception\InvalidPayloadException;
use Fusio\Worker\Runtime\Exception\RuntimeException;
use PSX\Record\Record;
use PSX\Schema\Exception\InvalidSchemaException;
use PSX\Schema\Exception\ValidationException;
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

    /**
     * @throws RuntimeException
     */
    public function run(string $actionFile, \stdClass|Execute $payload): Response
    {
        if ($payload instanceof \stdClass) {
            $payload = $this->parseExecute($payload);
        }

        $connector = new Connector($payload->getConnections() ?? new Record());
        $dispatcher = new Dispatcher();
        $logger = new Logger();
        $responseBuilder = new ResponseBuilder();

        if (!is_file($actionFile)) {
            throw new FileNotFoundException('Provided action files does not exist');
        }

        $handler = require $actionFile;
        if (!is_callable($handler)) {
            throw new InvalidActionException('Provided action does not return a callable');
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

    /**
     * @throws InvalidPayloadException
     */
    private function parseExecute(\stdClass $payload): Execute
    {
        try {
            $schema = (new SchemaManager())->getSchema(Execute::class);
            $execute = (new SchemaTraverser())->traverse($payload, $schema, new TypeVisitor());

            if (!$execute instanceof Execute) {
                throw new InvalidSchemaException('Could not read execute payload');
            }

            return $execute;
        } catch (InvalidSchemaException|ValidationException $e) {
            throw new InvalidPayloadException('Could not read execute payload', previous: $e);
        }
    }
}
