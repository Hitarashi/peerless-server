//! Single source of truth for command discovery.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandRole {
    User,
    Admin,
}

#[derive(Debug, Clone, Copy)]
pub struct CommandDescriptor {
    pub name: &'static str,
    pub role: CommandRole,
    pub summary: &'static str,
    pub usage: &'static str,
}

pub const COMMANDS: &[CommandDescriptor] = &[
    CommandDescriptor {
        name: "start",
        role: CommandRole::User,
        summary: "Show the quick start guide",
        usage: "/start",
    },
    CommandDescriptor {
        name: "get",
        role: CommandRole::User,
        summary: "Download primary audio plus optional Dolby Atmos",
        usage: "/get <Apple Music link> [-f]",
    },
    CommandDescriptor {
        name: "search",
        role: CommandRole::User,
        summary: "Search cached tracks and the Apple Music catalog",
        usage: "/search <query>",
    },
    CommandDescriptor {
        name: "status",
        role: CommandRole::User,
        summary: "View active downloads",
        usage: "/status",
    },
    CommandDescriptor {
        name: "info",
        role: CommandRole::User,
        summary: "Inspect metadata and cache status",
        usage: "/info <Apple Music link>",
    },
    CommandDescriptor {
        name: "spec",
        role: CommandRole::User,
        summary: "Generate a spectrogram from replied audio",
        usage: "/spec",
    },
    CommandDescriptor {
        name: "report",
        role: CommandRole::User,
        summary: "Report a problem with a track",
        usage: "/report",
    },
    CommandDescriptor {
        name: "help",
        role: CommandRole::User,
        summary: "Show this guide",
        usage: "/help",
    },
    CommandDescriptor {
        name: "settings",
        role: CommandRole::Admin,
        summary: "Change bot operating settings",
        usage: "/settings",
    },
    CommandDescriptor {
        name: "delete",
        role: CommandRole::Admin,
        summary: "Delete a cached track",
        usage: "/delete <Apple Music link>",
    },
    CommandDescriptor {
        name: "auth",
        role: CommandRole::Admin,
        summary: "Authorize a user or group",
        usage: "/auth <user_id | reply>",
    },
    CommandDescriptor {
        name: "revoke",
        role: CommandRole::Admin,
        summary: "Revoke authorization",
        usage: "/revoke <user_id | reply>",
    },
    CommandDescriptor {
        name: "authlist",
        role: CommandRole::Admin,
        summary: "List authorized users",
        usage: "/authlist",
    },
    CommandDescriptor {
        name: "stats",
        role: CommandRole::Admin,
        summary: "View download and cache statistics",
        usage: "/stats",
    },
    CommandDescriptor {
        name: "clean",
        role: CommandRole::Admin,
        summary: "Remove leftover temporary files",
        usage: "/clean",
    },
    CommandDescriptor {
        name: "ping",
        role: CommandRole::Admin,
        summary: "Check Telegram, database, and mirror health",
        usage: "/ping",
    },
    CommandDescriptor {
        name: "index",
        role: CommandRole::Admin,
        summary: "Reconcile the dump channel with the database",
        usage: "/index",
    },
    CommandDescriptor {
        name: "export",
        role: CommandRole::Admin,
        summary: "Export a database archive",
        usage: "/export",
    },
    CommandDescriptor {
        name: "import",
        role: CommandRole::Admin,
        summary: "Restore a database archive",
        usage: "/import",
    },
];

pub fn render_help(is_admin: bool) -> String {
    let mut out = String::from("<b>Peerless</b>\n\n<b>Start here</b>\n");
    for command in COMMANDS
        .iter()
        .filter(|command| command.role == CommandRole::User)
    {
        out.push_str(&format!(
            "• {} — {}\n",
            display_usage(command.usage),
            command.summary
        ));
    }
    out.push_str(
        "\n<i>/get delivers the highest available primary rendition plus optional Dolby Atmos. Multi-track albums are delivered as ZIP archives; missing Atmos is silent. Audio requested in a group is delivered to your private chat.</i>",
    );
    if is_admin {
        out.push_str("\n\n<b>Admin commands</b>\n");
        for command in COMMANDS
            .iter()
            .filter(|command| command.role == CommandRole::Admin)
        {
            out.push_str(&format!(
                "• {} — {}\n",
                display_usage(command.usage),
                command.summary
            ));
        }
    }
    out
}

fn display_usage(usage: &str) -> String {
    usage.replace('<', "[").replace('>', "]")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn catalog_is_unique_and_role_aware() {
        let mut names = COMMANDS
            .iter()
            .map(|command| command.name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), COMMANDS.len());
        assert!(!render_help(false).contains("/settings"));
        assert!(render_help(true).contains("/settings"));
        assert!(render_help(false).contains("cached tracks and the Apple Music catalog"));
        assert!(render_help(false).contains("/get [Apple Music link]"));
        assert!(render_help(false).contains("optional Dolby Atmos"));
        assert!(render_help(false).contains("Multi-track albums are delivered as ZIP archives"));
        assert!(!render_help(false).contains("/zip"));
        assert!(!render_help(false).contains("/alac"));
        assert!(!render_help(false).contains("/rip"));
        assert!(!render_help(false).contains("/cancel"));
        assert!(!render_help(false).contains("<link | id>"));
    }

    #[test]
    fn catalog_contains_only_canonical_commands() {
        let names = COMMANDS
            .iter()
            .map(|command| command.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "start", "get", "search", "status", "info", "spec", "report", "help", "settings",
                "delete", "auth", "revoke", "authlist", "stats", "clean", "ping", "index",
                "export", "import",
            ]
        );
    }
}
