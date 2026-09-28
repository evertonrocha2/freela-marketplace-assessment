package br.com.freela.contrato.application;

import br.com.freela.common.correlation.CorrelationContext;
import br.com.freela.contrato.application.port.RegistroDeEventos;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.ContratoNaoEncontradoException;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Casos de uso do ciclo de vida do contrato.
 *
 * <p>Toda alteracao segue o mesmo desenho: abre transacao, carrega o agregado com lock, aplica a
 * regra, persiste o estado e grava os eventos resultantes na outbox. Persistencia e evento vivem
 * na mesma transacao, entao ou os dois acontecem ou nenhum acontece.</p>
 */
@Service
public class ContratoApplicationService {

    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);

    private final ContratoRepository repository;
    private final RegistroDeEventos registroDeEventos;

    public ContratoApplicationService(ContratoRepository repository, RegistroDeEventos registroDeEventos) {
        this.repository = repository;
        this.registroDeEventos = registroDeEventos;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        String correlationId = CorrelationContext.atualOuNovo();
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());

        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        log.info("contrato.dominio.criado contratoId={} status={} eventosPendentes={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());

        Contrato salvo = repository.salvar(contrato);
        registroDeEventos.registrar(contrato.pullDomainEvents(), correlationId);

        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        return aplicar(id, "entrega", Contrato::registrarEntrega);
    }

    @Transactional
    public Contrato concluir(UUID id) {
        return aplicar(id, "conclusao", Contrato::concluir);
    }

    @Transactional
    public Contrato cancelar(UUID id, String motivo) {
        return aplicar(id, "cancelamento", contrato -> contrato.cancelar(motivo));
    }

    private Contrato aplicar(UUID id, String operacao, Consumer<Contrato> transicao) {
        String correlationId = CorrelationContext.atualOuNovo();
        log.info("contrato.{}.inicio contratoId={}", operacao, id);

        Contrato contrato = repository.buscarPorIdParaAtualizacao(id)
                .orElseThrow(() -> new ContratoNaoEncontradoException(id));
        transicao.accept(contrato);

        Contrato salvo = repository.salvar(contrato);
        registroDeEventos.registrar(contrato.pullDomainEvents(), correlationId);

        log.info("contrato.{}.sucesso contratoId={} status={}", operacao, salvo.id(), salvo.status());
        return salvo;
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new ContratoNaoEncontradoException(id));
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }
}
