<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\Amqp\Channel;
use Thesis\Amqp\Client;
use Thesis\Amqp\DeliveryMessage;
use Thesis\Amqp\PublishMessage;
use Thesis\Message\Command;
use Thesis\MessageBus\Envelope;
use Thesis\MessageBus\Transport\Transport;
use Thesis\Sync\Once;

/**
 * @api
 */
final class AmqpTransport implements Transport
{
    /**
     * @var Once<Channel>
     */
    private readonly Once $publishChannel;

    public function __construct(
        private readonly Client $client,
        private readonly ExchangeNaming $exchangeNaming = new MessageClassBasedExchangeNaming(),
        private readonly AmqpEnvelopeEncoder $encoder = new DefaultAmqpEnvelopeEncoder(),
    ) {
        $this->publishChannel = new Once(
            function: static function () use ($client): Channel {
                $channel = $client->channel();
                $channel->confirmSelect();

                return $channel;
            },
            isAlive: static fn(Channel $channel): bool => !$channel->isClosed(),
        );
    }

    public function setup(string $endpoint, array $localMessages): void
    {
        $channel = $this->client->channel();
        $channel->queueDeclare($endpoint, durable: true);

        foreach ($localMessages as $localMessage) {
            $exchange = $this->exchangeNaming->nameExchange($localMessage);
            $this->declareExchange($channel, $exchange);
            $channel->queueBind($endpoint, $exchange);
        }

        $channel->close();
    }

    public function publish(array $envelopes): void
    {
        $this
            ->publishChannel
            ->await()
            ->publishBatch(array_map(
                function (Envelope $envelope): PublishMessage {
                    $exchange = $this->exchangeNaming->nameExchange($envelope->messageClass);
                    $this->declareExchange($this->publishChannel->await(), $exchange);

                    return new PublishMessage(
                        message: $this->encoder->encodeEnvelope($envelope),
                        exchange: $exchange,
                        mandatory: $envelope->message instanceof Command,
                    );
                },
                $envelopes,
            ))
            ->await()
            ->ensureAllPublished();
    }

    public function consume(string $endpoint, \Closure $handler): \Closure
    {
        $client = new Client($this->client->config);
        $channel = $client->channel();
        $channel->qos(prefetchCount: 1);

        $consumerTag = $channel->consume(
            callback: function (DeliveryMessage $deliveryMessage) use ($handler): void {
                $handler($this->encoder->decodeEnvelope($deliveryMessage->message));
                $deliveryMessage->ack();
            },
            queue: $endpoint,
        );

        return static function () use ($client, $channel, $consumerTag): void {
            $channel->cancel($consumerTag);
            $channel->close();
            $client->disconnect();
        };
    }

    /**
     * @var array<non-empty-string, Once<void>>
     */
    private array $declaredExchanges = [];

    /**
     * @param non-empty-string $exchange
     */
    private function declareExchange(Channel $channel, string $exchange): void
    {
        $this->declaredExchanges[$exchange] ??= new Once(
            static function () use ($channel, $exchange): void {
                $channel->exchangeDeclare($exchange, exchangeType: 'fanout', durable: true);
            },
        );
        $this->declaredExchanges[$exchange]->await();
    }
}
