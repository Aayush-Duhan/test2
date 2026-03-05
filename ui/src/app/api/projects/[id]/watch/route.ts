import { NextRequest, NextResponse } from 'next/server';
import { getProject } from '@/lib/storage';
import { subscribeToProject, type FileChangeEvent } from '@/lib/project-watcher';

export const runtime = 'nodejs';

async function getParamsId(
    params: { id: string } | Promise<{ id: string }>
): Promise<string> {
    const resolved = await params;
    return resolved.id;
}

/**
 * GET /api/projects/[id]/watch
 *
 * Server-Sent Events stream that pushes `files:changed` events whenever
 * files inside the project's watched directories are added, modified, or
 * deleted.  The frontend subscribes to this instead of polling.
 */
export async function GET(
    _request: NextRequest,
    { params }: { params: { id: string } | Promise<{ id: string }> },
) {
    const id = await getParamsId(params);

    const project = await getProject(id);
    if (!project) {
        return NextResponse.json({ error: 'Project not found' }, { status: 404 });
    }

    const projectName = project.name;

    const stream = new ReadableStream({
        start(controller) {
            const encoder = new TextEncoder();

            const push = (text: string) => {
                try {
                    controller.enqueue(encoder.encode(text));
                } catch {
                    /* stream already closed */
                }
            };

            // Heartbeat every 20s to keep the connection alive
            const heartbeat = setInterval(() => {
                push(': heartbeat\n\n');
            }, 20_000);

            const unsubscribe = subscribeToProject(projectName, (event: FileChangeEvent) => {
                push(`event: files:changed\ndata: ${JSON.stringify(event)}\n\n`);
            });

            // Cleanup when the client disconnects
            _request.signal.addEventListener('abort', () => {
                clearInterval(heartbeat);
                unsubscribe();
            });
        },
    });

    return new Response(stream, {
        headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            Connection: 'keep-alive',
        },
    });
}
