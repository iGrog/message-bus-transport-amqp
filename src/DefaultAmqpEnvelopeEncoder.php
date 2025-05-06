<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\Amqp\DeliveryMode;
use Thesis\Amqp\Message;
use Thesis\MessageBus\Encoding\EncodedData;
use Thesis\MessageBus\Encoding\Encoder;
use Thesis\MessageBus\Encoding\JsonEncoder;
use Thesis\MessageBus\Encoding\MessageClassEncoder;
use Thesis\MessageBus\Encoding\ObjectNormalizer;
use Thesis\MessageBus\Encoding\PassthroughClassEncoder;
use Thesis\MessageBus\Encoding\PHPSerializeObjectNormalizer;
use Thesis\MessageBus\Envelope;
use Thesis\MessageBus\Stamp;
use Thesis\MessageBus\Tracing\CauseId;
use Thesis\MessageBus\Tracing\CorrelationId;
use Thesis\MessageBus\Tracing\MessageId;
use Thesis\MessageBus\Tracing\SourceEndpoint;
use Thesis\MessageBus\Tracing\Timestamp;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class DefaultAmqpEnvelopeEncoder implements AmqpEnvelopeEncoder
{
    public const array DEFAULT_STAMP_HEADERS = [
        CauseId::class => 'x-cause-id',
        SourceEndpoint::class => 'x-source-endpoint',
    ];

    /**
     * @param array<class-string<Stamp>, string> $stampHeaders
     */
    public function __construct(
        private MessageClassEncoder $messageClassEncoder = new PassthroughClassEncoder(),
        private ObjectNormalizer $objectNormalizer = new PHPSerializeObjectNormalizer(),
        private Encoder $encoder = new JsonEncoder(),
        private array $stampHeaders = self::DEFAULT_STAMP_HEADERS,
    ) {}

    public function encodeEnvelope(Envelope $envelope): Message
    {
        $encodedData = $this->encoder->encode($this->objectNormalizer->normalizeObject($envelope->message));
        $expiration = $envelope->findStamp(AmqpExpiration::class);

        return new Message(
            body: $encodedData->data,
            headers: $this->stampsToHeaders(
                $envelope
                    ->withoutStamps([
                        MessageId::class,
                        CorrelationId::class,
                        Timestamp::class,
                        AmqpDeliveryMode::class,
                        AmqpPriority::class,
                        AmqpExpiration::class,
                        AmqpAppId::class,
                    ])
                    ->stamps,
            ),
            contentType: $encodedData->type,
            contentEncoding: $encodedData->encoding,
            deliveryMode: $envelope->findStamp(AmqpDeliveryMode::class)->deliveryMode ?? DeliveryMode::Persistent,
            priority: $envelope->findStamp(AmqpPriority::class)?->priority,
            correlationId: $envelope->findStamp(CorrelationId::class)?->correlationId,
            expiration: $expiration === null ? null : (string) $expiration->expiration->toMilliseconds(),
            messageId: $envelope->findStamp(MessageId::class)?->messageId,
            timestamp: $envelope->findStamp(Timestamp::class)?->timestamp,
            type: $this->messageClassEncoder->encodeMessageClass($envelope->messageClass),
            appId: $envelope->findStamp(AmqpAppId::class)?->appId,
        );
    }

    /**
     * @param list<Stamp> $stamps
     * @return array<string, mixed>
     */
    private function stampsToHeaders(array $stamps): array
    {
        $headers = [];

        foreach ($stamps as $stamp) {
            $name = $this->stampHeaders[$stamp::class] ?? $stamp::class;
            $headers[$name] = $this->objectNormalizer->normalizeObject($stamp);
        }

        return $headers;
    }

    public function decodeEnvelope(Message $message): Envelope
    {
        if ($message->type === null) {
            throw new \LogicException('No message type');
        }

        $stamps = $this->headersToStamps($message->headers);

        if ($message->messageId !== null && $message->messageId !== '') {
            $stamps[] = new MessageId($message->messageId);
        }

        if ($message->correlationId !== null && $message->correlationId !== '') {
            $stamps[] = new CorrelationId($message->correlationId);
        }

        if ($message->timestamp !== null) {
            $stamps[] = new Timestamp($message->timestamp);
        }

        if ($message->deliveryMode !== DeliveryMode::Whatever) {
            $stamps[] = new AmqpDeliveryMode($message->deliveryMode);
        }

        if ($message->priority !== null) {
            $stamps[] = new AmqpPriority($message->priority);
        }

        if ($message->expiration !== null) {
            $stamps[] = new AmqpExpiration(TimeSpan::fromMilliseconds((int) $message->expiration));
        }

        if ($message->appId !== null) {
            $stamps[] = new AmqpAppId($message->appId);
        }

        return new Envelope(
            message: $this->objectNormalizer->denormalizeObject(
                class: $this->messageClassEncoder->decodeMessageClass($message->type),
                data: $this->encoder->decode(
                    new EncodedData(
                        data: $message->body,
                        type: $message->contentType,
                        encoding: $message->contentEncoding,
                    ),
                ),
            ),
            stamps: $stamps,
        );
    }

    /**
     * @param array<string, mixed> $headers
     * @return list<Stamp>
     */
    private function headersToStamps(array $headers): array
    {
        $headerStamps = array_flip($this->stampHeaders);
        $stamps = [];

        foreach ($headers as $name => $value) {
            $class = $headerStamps[$name] ?? $name;

            if (!is_a($class, Stamp::class, allow_string: true)) {
                continue;
            }

            $stamps[] = $this->objectNormalizer->denormalizeObject($class, $value);
        }

        return $stamps;
    }
}
