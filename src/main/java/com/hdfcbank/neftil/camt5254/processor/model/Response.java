package com.hdfcbank.neftil.camt5254.processor.model;

import lombok.*;

@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Response {

    String status;
    String message;
}
