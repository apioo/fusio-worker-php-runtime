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
use PSX\Http\Environment\HttpResponseInterface;
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

        $return = call_user_func_array($handler, [
            $payload->getRequest(),
            $payload->getContext(),
            $connector,
            $responseBuilder,
            $dispatcher,
            $logger
        ]);

        if ($return instanceof HttpResponseInterface) {
            $response = new ResponseHTTP();
            $response->setStatusCode($return->getStatusCode());
            $response->setHeaders(Record::fromArray($return->getHeaders()));
            $response->setBody($return->getBody());
        } else {
            $response = new ResponseHTTP();
            $response->setStatusCode(204);
        }

        $workerResponse = new Response();
        $workerResponse->setEvents($dispatcher->getEvents());
        $workerResponse->setLogs($logger->getLogs());
        $workerResponse->setResponse($response);

        return $workerResponse;
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
