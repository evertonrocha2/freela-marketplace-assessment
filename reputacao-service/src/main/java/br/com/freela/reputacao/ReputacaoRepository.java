package br.com.freela.reputacao;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ReputacaoRepository extends JpaRepository<ReputacaoFreelancer, UUID> {

    /**
     * Le a reputacao com trava de escrita ({@code SELECT ... FOR UPDATE}).
     *
     * <p>A particao do Kafka e definida pelo contratoId, nao pelo freelancerId. Dois contratos do
     * mesmo freelancer podem estar em particoes diferentes e ser consumidos em paralelo. Sem a
     * trava, as duas threads leem o mesmo contador, somam um cada e a ultima gravacao apaga a
     * outra. Com ela, a segunda thread espera a primeira confirmar e le o valor ja atualizado.</p>
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select r from ReputacaoFreelancer r where r.freelancerId = :freelancerId")
    Optional<ReputacaoFreelancer> buscarParaAtualizar(@Param("freelancerId") UUID freelancerId);
}
