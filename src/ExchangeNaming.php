<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\Message\Message;

/**
 * @api
 */
interface ExchangeNaming
{
    /**
     * @param class-string<Message> $message
     * @return non-empty-string
     */
    public function nameExchange(string $message): string;
}
