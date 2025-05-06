<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\Amqp\Message;
use Thesis\MessageBus\Envelope;

/**
 * @api
 */
interface AmqpEnvelopeEncoder
{
    public function encodeEnvelope(Envelope $envelope): Message;

    public function decodeEnvelope(Message $message): Envelope;
}
