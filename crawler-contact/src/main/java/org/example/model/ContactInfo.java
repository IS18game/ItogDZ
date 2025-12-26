package org.example.model;

import jakarta.persistence.*;

@Entity
@Table(name = "contact_info")
public class ContactInfo {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String sourceUrl;
    private String phone;
    private String email;
    private String address;

    public ContactInfo() {}

    public ContactInfo(String sourceUrl, String phone, String email, String address) {
        this.sourceUrl = sourceUrl;
        this.phone = phone;
        this.email = email;
        this.address = address;
    }

    public Long getId() { return id; }

    public String getSourceUrl() { return sourceUrl; }
    public void setSourceUrl(String sourceUrl) { this.sourceUrl = sourceUrl; }

    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
}