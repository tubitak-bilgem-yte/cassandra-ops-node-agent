package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.remote;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.command.AsyncBackupCommand;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.RestoreService;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity.RestoreRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@RestController
@RequiredArgsConstructor
public class RestoreController {

	private final RestoreService restoreService;
	private final AsyncBackupCommand asyncBackupCommand;

	@PostMapping("/restore")
	public void restore(@RequestBody final RestoreRequest restoreRequest) {
		restoreService.restore(restoreRequest);
	}

	@GetMapping("/backupDirectories")
	public List<String> getBackupDirectoriesFromBackupPath(@RequestParam final String backupPath) throws IOException {
		return restoreService.getBackupDirectoriesForGivenBackupName(backupPath);
	}


	@PostMapping("/backupRelations")
	public Set<String> backupRelations(@RequestBody final String backupPath) throws IOException {
		return asyncBackupCommand.getBackupRelationsFromBackupPath(backupPath);
	}
}
