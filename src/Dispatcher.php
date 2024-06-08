<?php

namespace Fusio\Worker\Runtime;

use Fusio\Engine\DispatcherInterface;
use Fusio\Worker\ResponseEvent;

class Dispatcher implements DispatcherInterface
{
    /**
     * @var ResponseEvent[]
     */
    private array $events = [];

    public function dispatch(string $eventName, mixed $payload): void
    {
        $event = new ResponseEvent();
        $event->setEventName($eventName);
        $event->setData($payload);

        $this->events[] = $event;
    }

    public function getEvents(): array
    {
        return $this->events;
    }
}
