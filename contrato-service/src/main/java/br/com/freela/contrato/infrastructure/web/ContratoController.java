package br.com.freela.contrato.infrastructure.web;

import br.com.freela.common.correlation.CorrelationContext;
import br.com.freela.common.correlation.EscopoMdc;
import br.com.freela.common.correlation.MdcKeys;
import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/contratos")
public class ContratoController {

    private static final Logger log = LoggerFactory.getLogger(ContratoController.class);

    private final ContratoApplicationService service;

    public ContratoController(ContratoApplicationService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ContratoResponse criar(@Valid @RequestBody CriarContratoRequest request) {
        log.info("http.contrato.criar clienteId={} freelancerId={} titulo={}",
                request.clienteId(), request.freelancerId(), request.titulo());
        var contrato = service.criar(new CriarContratoCommand(
                request.clienteId(), request.freelancerId(), request.titulo(), request.valor()));
        log.info("http.contrato.criar.response contratoId={} status={} correlationId={}",
                contrato.id(), contrato.status(), CorrelationContext.atual());
        return ContratoResponse.from(contrato);
    }

    @PostMapping("/{id}/entregas")
    public ContratoResponse registrarEntrega(@PathVariable UUID id) {
        return comContratoNoLog(id, () -> {
            log.info("http.contrato.entrega contratoId={}", id);
            return ContratoResponse.from(service.registrarEntrega(id));
        });
    }

    @PostMapping("/{id}/conclusao")
    public ContratoResponse concluir(@PathVariable UUID id) {
        return comContratoNoLog(id, () -> {
            log.info("http.contrato.conclusao contratoId={}", id);
            return ContratoResponse.from(service.concluir(id));
        });
    }

    @PostMapping("/{id}/cancelamento")
    public ContratoResponse cancelar(@PathVariable UUID id,
                                     @RequestBody(required = false) CancelarContratoRequest request) {
        return comContratoNoLog(id, () -> {
            String motivo = request == null ? null : request.motivo();
            log.info("http.contrato.cancelamento contratoId={}", id);
            return ContratoResponse.from(service.cancelar(id, motivo));
        });
    }

    @GetMapping("/{id}")
    public ContratoResponse buscar(@PathVariable UUID id) {
        return comContratoNoLog(id, () -> {
            log.info("http.contrato.buscar contratoId={}", id);
            return ContratoResponse.from(service.buscar(id));
        });
    }

    @GetMapping
    public List<ContratoResponse> listar() {
        log.info("http.contrato.listar");
        return service.listar().stream().map(ContratoResponse::from).toList();
    }

    private <T> T comContratoNoLog(UUID contratoId, Supplier<T> acao) {
        try (EscopoMdc escopo = EscopoMdc.de(Map.of(MdcKeys.CONTRATO_ID, contratoId.toString()))) {
            return acao.get();
        }
    }
}
