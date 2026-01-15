import { ReactNode } from "react";

interface FormattedAnswerProps {
  content: string;
  className?: string;
}

export function FormattedAnswer({ content, className = "" }: FormattedAnswerProps) {
  // Parse and format the answer with proper markdown-style rendering
  const formatContent = (text: string): ReactNode[] => {
    const lines = text.split('\n');
    const elements: ReactNode[] = [];
    let currentList: { type: 'bullet' | 'numbered'; items: Array<{ number?: number; text: string }> } | null = null;
    let listKey = 0;

    const flushList = () => {
      if (currentList) {
        if (currentList.type === 'bullet') {
          elements.push(
            <ul key={`list-${listKey++}`} className="my-3 ml-4 space-y-1.5">
              {currentList.items.map((item, idx) => (
                <li key={idx} className="flex items-start gap-2">
                  <span className="text-purple-600 dark:text-purple-400 mt-0.5 flex-shrink-0">•</span>
                  <span className="whitespace-pre-wrap">{formatInlineText(item.text)}</span>
                </li>
              ))}
            </ul>
          );
        } else {
          elements.push(
            <ol key={`list-${listKey++}`} className="my-3 ml-4 space-y-1.5">
              {currentList.items.map((item, idx) => (
                <li key={idx} className="flex items-start gap-2">
                  <span className="text-purple-600 dark:text-purple-400 font-semibold mt-0.5 flex-shrink-0 min-w-[1.5rem]">
                    {item.number !== undefined ? `${item.number}.` : `${idx + 1}.`}
                  </span>
                  <span className="whitespace-pre-wrap">{formatInlineText(item.text)}</span>
                </li>
              ))}
            </ol>
          );
        }
        currentList = null;
      }
    };

    const formatInlineText = (text: string): ReactNode[] => {
      const parts: ReactNode[] = [];
      let remaining = text;
      let key = 0;

      while (remaining.length > 0) {
        // Match **bold** text
        const boldMatch = remaining.match(/\*\*([^*]+)\*\*/);
        if (boldMatch && boldMatch.index !== undefined) {
          // Add text before bold
          if (boldMatch.index > 0) {
            parts.push(remaining.substring(0, boldMatch.index));
          }
          // Add bold text
          parts.push(
            <strong key={`bold-${key++}`} className="font-semibold text-purple-800 dark:text-purple-200">
              {boldMatch[1]}
            </strong>
          );
          remaining = remaining.substring(boldMatch.index + boldMatch[0].length);
        } else {
          parts.push(remaining);
          break;
        }
      }

      return parts;
    };

    lines.forEach((line, idx) => {
      const trimmed = line.trim();

      // Empty line - flush list and add spacing
      if (!trimmed) {
        flushList();
        if (elements.length > 0) {
          elements.push(<div key={`space-${idx}`} className="h-2" />);
        }
        return;
      }

      // Continuation line - indented text following a list item
      if (line.startsWith('  ') && currentList && currentList.items.length > 0) {
        // Append to the last list item
        const lastItem = currentList.items[currentList.items.length - 1];
        lastItem.text += '\n' + trimmed;
        return;
      }

      // Heading (line ending with :)
      if (trimmed.endsWith(':') && !trimmed.match(/^[-•\d]/)) {
        flushList();
        elements.push(
          <h4 key={`heading-${idx}`} className="font-bold text-purple-900 dark:text-purple-100 mt-4 mb-2 first:mt-0 text-[0.9375rem]">
            {formatInlineText(trimmed)}
          </h4>
        );
        return;
      }

      // Bullet point (starts with - or •)
      const bulletMatch = trimmed.match(/^[-•]\s+(.+)/);
      if (bulletMatch) {
        if (!currentList || currentList.type !== 'bullet') {
          flushList();
          currentList = { type: 'bullet', items: [] };
        }
        currentList.items.push({ text: bulletMatch[1] });
        return;
      }

      // Numbered list (starts with number.)
      const numberedMatch = trimmed.match(/^(\d+)\.\s+(.+)/);
      if (numberedMatch) {
        const number = parseInt(numberedMatch[1], 10);
        if (!currentList || currentList.type !== 'numbered') {
          flushList();
          currentList = { type: 'numbered', items: [] };
        }
        currentList.items.push({ number, text: numberedMatch[2] });
        return;
      }

      // Regular paragraph
      flushList();
      elements.push(
        <p key={`para-${idx}`} className="my-2.5 leading-relaxed first:mt-0">
          {formatInlineText(trimmed)}
        </p>
      );
    });

    flushList();
    return elements;
  };

  return (
    <div className={`text-sm text-gray-800 dark:text-gray-200 ${className}`}>
      {formatContent(content)}
    </div>
  );
}
