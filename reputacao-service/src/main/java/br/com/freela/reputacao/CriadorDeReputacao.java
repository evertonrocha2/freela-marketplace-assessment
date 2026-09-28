package br.com.freela.reputacao;

import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Cria o registro de reputacao de um freelancer que ainda nao tem nenhum.
 *
 * <p>Roda em transacao propria de proposito. A trava de escrita so protege uma linha que ja
 * existe; quando o freelancer e novo nao ha o que travar, e duas threads podem tentar inserir a
 * mesma chave ao mesmo tempo. Isolando a insercao, a que perder a corrida recebe a violacao de
 * chave aqui, e nao dentro da transacao que carrega o efeito do evento e a marca de idempotencia,
 * que seria abortada inteira.</p>
 */
@Component
class CriadorDeReputacao {

    private final ReputacaoRepository repository;

    CriadorDeReputacao(ReputacaoRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void criarSeAusente(UUID freelancerId) {
        if (!repository.existsById(freelancerId)) {
            repository.saveAndFlush(new ReputacaoFreelancer(freelancerId));
        }
    }
}
