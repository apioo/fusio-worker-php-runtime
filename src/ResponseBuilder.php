<?php

namespace Fusio\Worker\Runtime;

use Fusio\Engine\Response\FactoryInterface;
use PSX\Http\Environment\HttpResponse;
use PSX\Http\Environment\HttpResponseInterface;

class ResponseBuilder implements FactoryInterface
{
    public function build(int $statusCode, array $headers, mixed $body): HttpResponseInterface
    {
        return new HttpResponse($statusCode, $headers, $body);
    }
}
