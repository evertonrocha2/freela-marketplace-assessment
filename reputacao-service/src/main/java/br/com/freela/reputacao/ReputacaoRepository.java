package br.com.freela.reputacao;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReputacaoRepository extends JpaRepository<ReputacaoFreelancer, UUID> {
}
