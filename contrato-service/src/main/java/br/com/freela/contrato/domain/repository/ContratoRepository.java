package br.com.freela.contrato.domain.repository;

import br.com.freela.contrato.domain.model.Contrato;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ContratoRepository {

    Contrato salvar(Contrato contrato);

    Optional<Contrato> buscarPorId(UUID id);

    /**
     * Busca com lock exclusivo na linha do contrato.
     *
     * <p>E o que serializa duas alteracoes concorrentes no mesmo contrato. Sem isso, dois requests
     * simultaneos poderiam ler o mesmo estado, gravar dois eventos e a ordem na outbox nao refletir
     * a ordem do negocio.</p>
     */
    Optional<Contrato> buscarPorIdParaAtualizacao(UUID id);

    List<Contrato> listar();
}
