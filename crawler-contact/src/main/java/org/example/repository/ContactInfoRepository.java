package org.example.repository;

import org.example.model.ContactInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ContactInfoRepository extends JpaRepository<ContactInfo, Long> {

    boolean existsBySourceUrlAndEmail(String sourceUrl, String email);

    boolean existsBySourceUrlAndPhone(String sourceUrl, String phone);

    boolean existsBySourceUrlAndAddress(String sourceUrl, String address);
}