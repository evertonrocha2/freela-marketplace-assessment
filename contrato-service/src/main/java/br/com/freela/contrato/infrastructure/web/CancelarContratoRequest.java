package br.com.freela.contrato.infrastructure.web;

/** Corpo opcional do cancelamento. O motivo vai no payload do evento ContratoCancelado. */
public record CancelarContratoRequest(String motivo) {
}
