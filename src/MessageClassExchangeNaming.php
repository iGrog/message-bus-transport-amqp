<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

/**
 * @api
 */
final readonly class MessageClassExchangeNaming implements ExchangeNaming
{
    public function __construct(
        private string $namespaceSeparator = '.',
    ) {}

    public function nameExchange(string $message): string
    {
        /** @var non-empty-string */
        return str_replace('\\', $this->namespaceSeparator, $message);
    }
}
