package com.hdfcbank.neftil.camt5254.processor.model;


import lombok.*;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ReqPayload {

    private Header header;
    private Body body;
}
