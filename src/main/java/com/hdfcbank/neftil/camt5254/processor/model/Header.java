package com.hdfcbank.neftil.camt5254.processor.model;


import lombok.*;

@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Header {

    private String msgId;
    private String source;
    private String target;
    private String msgType;
    private String flowType;
    private boolean replayInd;
    private boolean invalidPayload;
    private String prefix;

}
