package br.com.freela.notificacao;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificacaoRepository extends JpaRepository<Notificacao, UUID> {

    List<Notificacao> findByContratoIdOrderByCriadaEmAsc(UUID contratoId);

    List<Notificacao> findByCorrelationIdOrderByCriadaEmAsc(String correlationId);
}
