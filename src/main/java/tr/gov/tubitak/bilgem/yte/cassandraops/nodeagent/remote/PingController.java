package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.remote;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PingController {

	@GetMapping
	@RequestMapping("ping")
	@ResponseStatus(HttpStatus.OK)
	public String getStatus() {
		return "OK";
	}
}
