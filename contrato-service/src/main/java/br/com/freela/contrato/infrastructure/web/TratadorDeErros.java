package br.com.freela.contrato.infrastructure.web;

import br.com.freela.common.correlation.CorrelationContext;
import br.com.freela.contrato.domain.model.ContratoNaoEncontradoException;
import br.com.freela.contrato.domain.model.TransicaoInvalidaException;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Converte as excecoes de dominio em respostas HTTP com o correlationId da operacao, para que o
 * erro devolvido ao cliente possa ser localizado nos logs centralizados.
 */
@RestControllerAdvice
public class TratadorDeErros {

    private static final Logger log = LoggerFactory.getLogger(TratadorDeErros.class);

    @ExceptionHandler(ContratoNaoEncontradoException.class)
    public ResponseEntity<Map<String, Object>> naoEncontrado(ContratoNaoEncontradoException e) {
        log.warn("http.erro.contrato-nao-encontrado mensagem={}", e.getMessage());
        return resposta(HttpStatus.NOT_FOUND, "CONTRATO_NAO_ENCONTRADO", e.getMessage());
    }

    @ExceptionHandler(TransicaoInvalidaException.class)
    public ResponseEntity<Map<String, Object>> transicaoInvalida(TransicaoInvalidaException e) {
        log.warn("http.erro.transicao-invalida contratoId={} statusAtual={} statusPretendido={}",
                e.contratoId(), e.statusAtual(), e.statusPretendido());
        return resposta(HttpStatus.CONFLICT, "TRANSICAO_INVALIDA", e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> argumentoInvalido(IllegalArgumentException e) {
        log.warn("http.erro.argumento-invalido mensagem={}", e.getMessage());
        return resposta(HttpStatus.BAD_REQUEST, "REQUISICAO_INVALIDA", e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> validacao(MethodArgumentNotValidException e) {
        String detalhe = e.getBindingResult().getFieldErrors().stream()
                .map(erro -> erro.getField() + ": " + erro.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("Requisição inválida");
        log.warn("http.erro.validacao detalhe={}", detalhe);
        return resposta(HttpStatus.BAD_REQUEST, "REQUISICAO_INVALIDA", detalhe);
    }

    private ResponseEntity<Map<String, Object>> resposta(HttpStatus status, String codigo, String mensagem) {
        return ResponseEntity.status(status).body(Map.of(
                "codigo", codigo,
                "mensagem", mensagem == null ? "" : mensagem,
                "correlationId", String.valueOf(CorrelationContext.atual()),
                "momento", Instant.now().toString()));
    }
}
